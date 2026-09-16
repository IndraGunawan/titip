package titip

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
)

// buildPurgeOperation parses a purge target URL/path and returns whether the target
// represents an exact O(1) primary key or pattern(s) for cache invalidation.
//
// Target formats:
//   - "/api/products"                   → isExact=false, keys=["p=/api/products:*"]
//   - "https://example.com/api/products"→ isExact=false, keys=["p=/api/products:h=example.com:*"] (or with scheme if IncludeProtocol)
//   - "https://example.com/api?id=42"   → isExact=true,  keys=["p=/api:h=example.com:qs=id=42:m=GET:"]
//   - "/api?id=42" (ExcludeHost: true)  → isExact=true,  keys=["p=/api:qs=id=42:m=GET:"]
//   - "/api?id=42" (ExcludeHost: false) → isExact=false, keys=["p=/api:h=*:qs=id=42:*"]
//   - "/"                               → isExact=false, keys=["p=/:*"] (homepage only)
func buildPurgeOperation(target string, cfg *CacheKey) (isExact bool, keys []string, err error) {
	if target == "" {
		return false, nil, nil
	}
	if cfg == nil {
		cfg = &CacheKey{}
	}

	// Parse URL
	var parsed *url.URL
	var scheme string
	if strings.HasPrefix(target, "/") {
		parsed, err = url.Parse(target)
		if err != nil {
			return false, nil, err
		}
	} else {
		if strings.HasPrefix(target, "https://") {
			scheme = "https"
		} else if strings.HasPrefix(target, "http://") {
			scheme = "http"
		}
		rawTarget := target
		if !strings.Contains(target, "://") {
			rawTarget = "http://" + target
		}
		parsed, err = url.Parse(rawTarget)
		if err != nil {
			return false, nil, err
		}
	}

	// Clean path using parsed.Path (raw unescaped) to avoid double-escaping
	rawPath := parsed.Path
	if rawPath == "" {
		rawPath = "/"
	}
	cleanedPath := path.Clean(rawPath)
	if cleanedPath == "." {
		cleanedPath = "/"
	} else if strings.HasSuffix(rawPath, "/") && !strings.HasSuffix(cleanedPath, "/") {
		cleanedPath += "/"
	}
	if cfg.CaseInsensitivePath {
		cleanedPath = strings.ToLower(cleanedPath)
	}

	host := normalizeHost(parsed.Host, scheme)

	buf := getBuffer()
	defer putBuffer(buf)

	buf.WriteString("p=")
	writeEscapedPath(buf, cleanedPath)

	// 1. Query String handling
	hasQuery := !cfg.ExcludeQueryString && parsed.RawQuery != ""
	if hasQuery {
		fakeURL, _ := url.Parse("http://x?" + parsed.RawQuery)
		fakeReq := &http.Request{
			Method: http.MethodGet,
			URL:    fakeURL,
			Header: http.Header{},
		}
		qs := buildQueryString(fakeReq, cfg)
		if qs != "" {
			buf.WriteByte(':')
			// If host is known OR ExcludeHost is true, it is an exact primary key!
			if cfg.ExcludeHost || host != "" {
				if !cfg.ExcludeHost && host != "" {
					buf.WriteString("h=")
					buf.WriteString(host)
					buf.WriteByte(':')
				}
				if cfg.IncludeProtocol && scheme != "" {
					buf.WriteString("s=")
					buf.WriteString(scheme)
					buf.WriteByte(':')
				}
				buf.WriteString("qs=")
				buf.WriteString(qs)

				if len(cfg.IncludedHeaderNames) == 0 && len(cfg.IncludedCookieNames) == 0 {
					buf.WriteString(":m=GET:")
					return true, []string{buf.String()}, nil
				}

				// If headers or cookies are part of cache key, pattern-match across those variants.
				buf.WriteString(":*")
				return false, []string{buf.String()}, nil
			}

			// Host is unknown and !cfg.ExcludeHost → pattern match across all hosts.
			// New order: p -> h -> s -> qs -> m
			buf.WriteString("h=*:")
			if cfg.IncludeProtocol && scheme != "" {
				buf.WriteString("s=")
				buf.WriteString(scheme)
				buf.WriteByte(':')
			}
			buf.WriteString("qs=")
			buf.WriteString(qs)
			buf.WriteString(":*")
			return false, []string{buf.String()}, nil
		}
	}

	// 2. Path All Variants (no query or query stripped)
	buf.WriteByte(':')
	if !cfg.ExcludeHost && host != "" {
		buf.WriteString("h=")
		buf.WriteString(host)
		buf.WriteByte(':')
		if cfg.IncludeProtocol && scheme != "" {
			buf.WriteString("s=")
			buf.WriteString(scheme)
			buf.WriteByte(':')
		}
		buf.WriteString("*")
		return false, []string{buf.String()}, nil
	}

	// Single pattern for all hosts/schemes/methods
	buf.WriteString("*")
	return false, []string{buf.String()}, nil
}

// buildPurgePrefixOperation parses a path/URL prefix and generates a Cloudflare-style prefix pattern.
//
// Prefix behavior:
//   - "/assets/" (with trailing slash)    → matches "p=/assets/*" (directory only, does not match "/assets-v2")
//   - "/assets"  (without trailing slash) → matches "p=/assets*"  (raw string prefix, matches "/assets", "/assets/...", and "/assets-v2")
//   - "/"                                 → matches "p=/*"        (matches entire cache; supports both hard & soft purge)
//   - ""                                  → error: empty prefix not allowed
func buildPurgePrefixOperation(prefix string, cfg *CacheKey) ([]string, error) {
	if prefix == "" {
		return nil, fmt.Errorf("titip: PurgePrefix does not accept empty prefix")
	}
	if cfg == nil {
		cfg = &CacheKey{}
	}

	var parsed *url.URL
	var scheme string
	var err error
	if strings.HasPrefix(prefix, "/") {
		parsed, err = url.Parse(prefix)
		if err != nil {
			return nil, err
		}
	} else {
		if strings.HasPrefix(prefix, "https://") {
			scheme = "https"
		} else if strings.HasPrefix(prefix, "http://") {
			scheme = "http"
		}
		rawPrefix := prefix
		if !strings.Contains(prefix, "://") {
			rawPrefix = "http://" + prefix
		}
		parsed, err = url.Parse(rawPrefix)
		if err != nil {
			return nil, err
		}
	}

	rawPath := parsed.Path
	if rawPath == "" {
		rawPath = "/"
	}
	cleanedPath := path.Clean(rawPath)
	if cleanedPath == "." {
		cleanedPath = "/"
	} else if strings.HasSuffix(rawPath, "/") && !strings.HasSuffix(cleanedPath, "/") {
		cleanedPath += "/"
	}
	if cfg.CaseInsensitivePath {
		cleanedPath = strings.ToLower(cleanedPath)
	}

	host := normalizeHost(parsed.Host, scheme)

	buf := getBuffer()
	defer putBuffer(buf)

	buf.WriteString("p=")
	writeEscapedPath(buf, cleanedPath)
	buf.WriteString("*")

	if !cfg.ExcludeHost && host != "" {
		buf.WriteString(":h=")
		buf.WriteString(host)
		buf.WriteByte(':')
		if cfg.IncludeProtocol && scheme != "" {
			buf.WriteString("s=")
			buf.WriteString(scheme)
			buf.WriteByte(':')
		}
		buf.WriteString("*")
		return []string{buf.String()}, nil
	}

	return []string{buf.String()}, nil
}
