package titip

import (
	"net/http"
	"net/url"
	"path"
	"strings"
)

// purgeMode identifies how a purge target should be executed.
type purgeMode int

const (
	// purgeModeExact: exact primary key match (path + specific query string).
	purgeModeExact purgeMode = iota

	// purgeModePathSweep: pattern match — purge a path and ALL its query variations.
	purgeModePathSweep

	// purgeModeWildcard: pattern match — purge all paths under a directory prefix.
	purgeModeWildcard
)

// purgeTarget holds a parsed purge request.
type purgeTarget struct {
	mode   purgeMode
	path   string // cleaned path (e.g. "/api/products")
	host   string // optional host scope (lowercased, default port stripped)
	scheme string // optional scheme scope ("http" or "https"); empty = all
	query  string // percent-encoded sorted query string for exact mode (e.g. "id%3D42%26page%3D1")
}

// parsePurgeTarget parses a raw purge target string into a structured purgeTarget.
//
// Supported formats:
//   - "/api/products"              → path sweep (all query variants)
//   - "/api/products?id=42"        → exact match (specific query variant only)
//   - "/assets/*"                  → wildcard directory purge
//   - "https://example.com/api"    → host-scoped path sweep
// parsePurgeTarget parses a raw purge target string into a structured purgeTarget,
// respecting the active KeyConfig rules (query filters, sorting, trailing slashes, host exclusion).
func parsePurgeTarget(target string, cfg *KeyConfig) (*purgeTarget, error) {
	if target == "" {
		return nil, nil
	}
	if cfg == nil {
		cfg = &KeyConfig{}
	}

	pt := &purgeTarget{}

	// Detect wildcard directory purge before URL parsing (the "/*" suffix).
	isWildcard := strings.HasSuffix(target, "/*") || target == "/"

	// Attempt URL parsing.
	var parsed *url.URL
	var err error
	if strings.HasPrefix(target, "/") {
		// Pure path (possibly with query or wildcard).
		parsed, err = url.Parse(target)
		if err != nil {
			return nil, err
		}
	} else {
		// May have a host or scheme. Try adding a scheme if missing.
		if !strings.Contains(target, "://") {
			parsed, err = url.Parse("http://" + target)
		} else {
			parsed, err = url.Parse(target)
		}
		if err != nil {
			return nil, err
		}
		// Only capture scheme when it was explicitly provided in original target.
		if strings.HasPrefix(target, "https://") {
			pt.scheme = "https"
		} else if strings.HasPrefix(target, "http://") {
			pt.scheme = "http"
		}
		pt.host = normalizeHost(parsed.Host, pt.scheme)
	}

	// Clean the path.
	rawPath := parsed.EscapedPath()
	if rawPath == "" {
		rawPath = "/"
	}

	if isWildcard {
		// Strip trailing "/*" or "/" to get the directory prefix.
		dir := strings.TrimSuffix(rawPath, "/*")
		dir = strings.TrimSuffix(dir, "/")
		if dir == "" {
			dir = "/"
		}
		pt.path = path.Clean(dir)
		pt.mode = purgeModeWildcard
		return pt, nil
	}

	cleanedPath := path.Clean(rawPath)
	if cleanedPath == "." {
		cleanedPath = "/"
	} else if strings.HasSuffix(rawPath, "/") && !strings.HasSuffix(cleanedPath, "/") {
		cleanedPath += "/"
	}
	pt.path = cleanedPath

	if !cfg.ExcludeQueryString && parsed.RawQuery != "" {
		// Exact query variant — build filtered/sorted query string matching KeyConfig.
		fakeURL, _ := url.Parse("http://x?" + parsed.RawQuery)
		fakeReq := &http.Request{
			Method: http.MethodGet,
			URL:    fakeURL,
			Header: http.Header{},
		}
		qs := buildQueryString(fakeReq, cfg)
		if qs != "" {
			pt.query = qs
			pt.mode = purgeModeExact
			return pt, nil
		}
	}

	// No query (or empty after filtering/exclusion) → sweep all variants for this path.
	pt.mode = purgeModePathSweep
	return pt, nil
}

// buildPurgePatterns generates the Redis glob patterns for a purge target.
//
// Pattern rules:
//   - Exact mode:       full primary key string (used for direct delete/soft-purge)
//   - PathSweep mode:   "meta:p=<path>[:h=<host>]:m=*"  (optionally with scheme suffix)
//   - Wildcard mode:    "meta:p=<path>/*"                (prefix match on path segment)
//
// When IncludeProtocol is true and no scheme is specified, two patterns are returned
// (one for http, one for https) to honour the dual-protocol rule.
func buildPurgePatterns(pt *purgeTarget, cfg *KeyConfig) []string {
	if pt == nil {
		return nil
	}

	switch pt.mode {
	case purgeModeExact:
		if !cfg.ExcludeHost && pt.host == "" {
			return buildSweepPatterns(pt, cfg)
		}
		return []string{buildExactKey(pt, cfg)}

	case purgeModePathSweep:
		return buildSweepPatterns(pt, cfg)

	case purgeModeWildcard:
		return buildWildcardPatterns(pt, cfg)
	}
	return nil
}

// buildExactKey constructs the full primary key string for exact-match purging.
// This matches what generatePrimaryKey would produce for the same request.
func buildExactKey(pt *purgeTarget, cfg *KeyConfig) string {
	var sb strings.Builder
	sb.WriteString("p=")
	sb.WriteString(pt.path)
	if !cfg.ExcludeHost && pt.host != "" {
		sb.WriteString(":h=")
		sb.WriteString(pt.host)
	}
	sb.WriteString(":m=GET")
	if cfg.IncludeProtocol && pt.scheme != "" {
		sb.WriteString(":s=")
		sb.WriteString(pt.scheme)
	}
	if pt.query != "" {
		sb.WriteString(":qs=")
		sb.WriteString(pt.query)
	}
	return sb.String()
}

// buildSweepPatterns returns patterns that match a path and ALL its query/method/scheme variants.
func buildSweepPatterns(pt *purgeTarget, cfg *KeyConfig) []string {
	base := buildPathHostBase(pt, cfg)
	qsSuffix := ""
	if pt.query != "" {
		qsSuffix = ":qs=" + pt.query + "*"
	}

	if cfg.IncludeProtocol && pt.scheme == "" {
		// Dual-protocol rule: emit patterns for both http and https.
		return []string{
			base + ":m=*:s=http*" + qsSuffix,
			base + ":m=*:s=https*" + qsSuffix,
		}
	}

	if cfg.IncludeProtocol && pt.scheme != "" {
		return []string{base + ":m=*:s=" + pt.scheme + "*" + qsSuffix}
	}

	// Protocol not included in key — wildcard covers method + optional qs/he/ck suffixes.
	return []string{base + ":m=*" + qsSuffix}
}

// buildWildcardPatterns returns patterns that match all cached paths under a directory prefix.
func buildWildcardPatterns(pt *purgeTarget, cfg *KeyConfig) []string {
	prefix := "p=" + pt.path
	if pt.path != "/" {
		prefix += "/"
	}

	if !cfg.ExcludeHost && pt.host != "" {
		// Host-scoped wildcard.
		if cfg.IncludeProtocol && pt.scheme == "" {
			return []string{
				prefix + "*:h=" + pt.host + ":m=*:s=http*",
				prefix + "*:h=" + pt.host + ":m=*:s=https*",
			}
		}
		if cfg.IncludeProtocol && pt.scheme != "" {
			return []string{prefix + "*:h=" + pt.host + ":m=*:s=" + pt.scheme + "*"}
		}
		return []string{prefix + "*:h=" + pt.host + ":m=*"}
	}

	// No host scope — match all hosts under this path prefix.
	return []string{prefix + "*"}
}

// buildPathHostBase builds the "p=<path>[:h=<host>]" prefix for sweep patterns.
func buildPathHostBase(pt *purgeTarget, cfg *KeyConfig) string {
	var sb strings.Builder
	sb.WriteString("p=")
	sb.WriteString(pt.path)
	if !cfg.ExcludeHost {
		if pt.host != "" {
			sb.WriteString(":h=")
			sb.WriteString(pt.host)
		} else {
			sb.WriteString(":h=*")
		}
	}
	return sb.String()
}

// normalizeHost lowercases the host and strips default ports.
func normalizeHost(host, scheme string) string {
	h := strings.ToLower(host)
	switch scheme {
	case "http":
		h = strings.TrimSuffix(h, ":80")
	case "https":
		h = strings.TrimSuffix(h, ":443")
	}
	return h
}
