package titip

import (
	"bytes"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
)

// defaultMarketingQueryParams contains commonly used advertising and tracking query parameters.
var defaultMarketingQueryParams = []string{
	"fbclid",
	"gclid",
	"igshid",
	"mc_cid",
	"mc_eid",
	"msclkid",
	"ttclid",
	"twclid",
	"utm_campaign",
	"utm_content",
	"utm_medium",
	"utm_source",
	"utm_term",
}

// CacheKey defines the rules for assembling zero-hash canonical cache keys.
//
// Every cached request automatically receives a cache key. A zero-value CacheKey{}
// or omitting WithCacheKey applies the standard RFC-compliant default:
// host included, protocol excluded, case-sensitive path, all query parameters
// retained, and sorted alphabetically.
type CacheKey struct {
	// IncludeProtocol includes the request scheme ("http" or "https") in the cache key.
	// When true, HTTP and HTTPS requests reference distinct cache entries.
	IncludeProtocol bool

	// ExcludeHost excludes the HTTP Host / domain from the cache key.
	// When true, Host is omitted so multiple domains serving identical content share cache entries.
	ExcludeHost bool

	// ExcludeQueryString removes all query parameters from the cache key.
	// When true, all query parameters are stripped so requests with different query strings share cache.
	ExcludeQueryString bool

	// DisableQueryStringSort preserves the original query parameter ordering from the request URL.
	// When true, query parameter order is preserved as received from the client.
	DisableQueryStringSort bool

	// IncludedQueryParams specifies an allowlist of query parameter names to include in the cache key.
	// If set, only these specific parameters are included in the cache key.
	IncludedQueryParams []string

	// ExcludedQueryParams specifies a denylist of query parameter names to exclude from the cache key.
	// If set, all query parameters except these are included in the cache key.
	ExcludedQueryParams []string

	// ExcludeMarketingParams filters out standard advertising and tracking query parameters
	// (e.g. utm_source, utm_campaign, utm_medium, gclid, fbclid, ttclid).
	// When true, marketing tracking parameters are stripped from the cache key.
	ExcludeMarketingParams bool

	// IncludedHeaderNames specifies request header names whose values are appended to the primary cache key.
	//
	// Note: Do NOT include headers that the origin already manages via the HTTP "Vary" header
	// (e.g. "Accept-Encoding"), as Titip handles origin Vary negotiation automatically.
	//
	// Warning: NEVER include authentication tokens or credentials (e.g. "Authorization").
	// Specifying headers with high cardinality or wide ranges of values dramatically lowers the
	// cache hit rate and causes higher eviction churn.
	//
	// Best used for low-cardinality headers or A/B experiment buckets (e.g. "X-Region", "X-Experiment-Bucket").
	IncludedHeaderNames []string

	// IncludedCookieNames specifies cookie names whose values are appended to the cache key.
	//
	// Warning: NEVER include session identifiers, auth cookies, or credentials.
	// Including unique per-user cookies effectively creates per-user caches, destroying hit rates.
	//
	// Best used for low-cardinality user preferences or A/B testing groups (e.g. "ab_group", "currency", "theme", "locale").
	IncludedCookieNames []string

	// CaseInsensitivePath normalizes the URL path to lowercase in the primary cache key.
	// When true, requests with different path casing (e.g. /Products/Shoes vs /products/shoes) share the same cache entry.
	CaseInsensitivePath bool

	// IncludedQueryParamValues specifies an allowlist of specific parameter values.
	// A parameter key in this map is only included in the cache key if its value matches one of the specified allowed values.
	// Any value not in the list is omitted from the cache key.
	IncludedQueryParamValues map[string][]string
}

// generatePrimaryKey constructs a canonical, zero-hash primary cache key for a request.
//
// Format: p=<path>:h=<host>:[s=<scheme>:][qs=<query>:]m=<method>:[he=<headers>:][ck=<cookies>:]
//
// Every component ends with a colon (:), providing strict segment boundaries for exact
// matches and collision-free pattern invalidation.
// Component ordering is fixed: path → host → scheme → query → method → headers → cookies.
// All component values are percent-encoded where they contain delimiter characters (:, =).
func generatePrimaryKey(r *http.Request, cfg *CacheKey) string {
	if cfg == nil {
		cfg = &CacheKey{}
	}

	buf := getBuffer()
	defer putBuffer(buf)

	// --- p=<path>: (always first) ---
	rawPath := "/"
	if r.URL != nil && r.URL.Path != "" {
		rawPath = r.URL.Path
	}
	// Clean the path (resolves ../, ./, double-slashes while preserving trailing slash).
	cleanedPath := path.Clean(rawPath)
	if cleanedPath == "." {
		cleanedPath = "/"
	} else if strings.HasSuffix(rawPath, "/") && !strings.HasSuffix(cleanedPath, "/") {
		cleanedPath += "/"
	}
	if cfg.CaseInsensitivePath {
		cleanedPath = strings.ToLower(cleanedPath)
	}

	buf.WriteString("p=")
	writeEscapedPath(buf, cleanedPath)
	buf.WriteByte(':')

	// --- h=<host>: (always second, unless excluded) ---
	if !cfg.ExcludeHost {
		host := r.Host
		if host == "" && r.URL != nil {
			host = r.URL.Host
		}
		if host != "" {
			host = normalizeHost(host, resolveScheme(r))
			if host != "" {
				buf.WriteString("h=")
				buf.WriteString(host)
				buf.WriteByte(':')
			}
		}
	}

	// --- s=<scheme>: (optional, only when IncludeProtocol == true) ---
	if cfg.IncludeProtocol {
		buf.WriteString("s=")
		buf.WriteString(resolveScheme(r))
		buf.WriteByte(':')
	}

	// --- qs=<query>: (optional, filtered and sorted) ---
	if !cfg.ExcludeQueryString && r.URL != nil && r.URL.RawQuery != "" {
		qs := buildQueryString(r, cfg)
		if qs != "" {
			buf.WriteString("qs=")
			buf.WriteString(qs)
			buf.WriteByte(':')
		}
	}

	// --- m=<method>: (always present; HEAD normalises to GET) ---
	method := r.Method
	if method == http.MethodHead || method == "" {
		method = http.MethodGet
	}
	buf.WriteString("m=")
	buf.WriteString(method)
	buf.WriteByte(':')

	// --- he=<headers>: (optional, percent-encoded values to prevent delimiter injection) ---
	if len(cfg.IncludedHeaderNames) > 0 {
		headers := slices.Clone(cfg.IncludedHeaderNames)
		slices.Sort(headers)
		for _, h := range headers {
			hLower := strings.ToLower(h)
			vals := r.Header.Values(h)
			if len(vals) == 0 {
				continue
			}
			buf.WriteString("he=")
			buf.WriteString(hLower)
			buf.WriteByte('~')
			for i, v := range vals {
				if i > 0 {
					buf.WriteByte(',')
				}
				// Percent-encode values to prevent : and = from colliding with key delimiters.
				buf.WriteString(url.QueryEscape(strings.TrimSpace(v)))
			}
			buf.WriteByte(':')
		}
	}

	// --- ck=<cookies>: (optional, percent-encoded values) ---
	if len(cfg.IncludedCookieNames) > 0 {
		cookieNames := slices.Clone(cfg.IncludedCookieNames)
		slices.Sort(cookieNames)
		for _, name := range cookieNames {
			cookie, err := r.Cookie(name)
			if err != nil || cookie == nil || cookie.Value == "" {
				continue
			}
			buf.WriteString("ck=")
			buf.WriteString(name)
			buf.WriteByte('~')
			buf.WriteString(url.QueryEscape(cookie.Value))
			buf.WriteByte(':')
		}
	}

	return buf.String()
}

// writeEscapedPath streams a cleaned path into buf, escaping each segment with
// url.PathEscape while preserving '/' hierarchy and trailing slashes.
// It guarantees that characters like '*' are percent-encoded as '%2A',
// ensuring no raw glob metacharacters exist in cache keys.
func writeEscapedPath(buf *bytes.Buffer, p string) {
	trimmed := strings.TrimPrefix(p, "/")
	if trimmed == "" {
		buf.WriteByte('/')
		return
	}
	for part := range strings.SplitSeq(trimmed, "/") {
		buf.WriteByte('/')
		if part != "" {
			buf.WriteString(url.PathEscape(part))
		}
	}
}

// resolveScheme determines the effective request scheme from TLS state and forwarded headers.
func resolveScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if r.Header != nil && r.Header.Get(headerXForwardedProto) == "https" {
		return "https"
	}
	if r.URL != nil && r.URL.Scheme == "https" {
		return "https"
	}
	return "http"
}

// buildQueryString assembles a filtered and sorted query string for inclusion in the cache key.
// The result is a raw query string that is safe to embed in the qs= label value.
func buildQueryString(r *http.Request, cfg *CacheKey) string {
	if cfg.ExcludeQueryString {
		return ""
	}
	if cfg.DisableQueryStringSort {
		return buildUnsortedQueryString(r, cfg)
	}
	return buildSortedQueryString(r, cfg)
}

// isQueryParamAllowed reports whether query param k with value v should be included per cfg.
func isQueryParamAllowed(k, v string, cfg *CacheKey) bool {
	hasIncludedParams := len(cfg.IncludedQueryParams) > 0 || len(cfg.IncludedQueryParamValues) > 0
	if hasIncludedParams {
		if slices.Contains(cfg.IncludedQueryParams, k) {
			return true
		}
		if len(cfg.IncludedQueryParamValues) > 0 {
			if allowedVals, ok := cfg.IncludedQueryParamValues[k]; ok {
				return slices.Contains(allowedVals, v)
			}
		}
		return false
	}
	if slices.Contains(cfg.ExcludedQueryParams, k) {
		return false
	}
	if cfg.ExcludeMarketingParams && slices.Contains(defaultMarketingQueryParams, strings.ToLower(k)) {
		return false
	}
	return true
}

// buildSortedQueryString parses, filters, sorts, and reassembles the query string.
func buildSortedQueryString(r *http.Request, cfg *CacheKey) string {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(values) == 0 {
		return ""
	}

	keys := make([]string, 0, len(values))
	for k, vals := range values {
		filteredVals := vals[:0]
		for _, v := range vals {
			if isQueryParamAllowed(k, v, cfg) {
				filteredVals = append(filteredVals, v)
			}
		}
		if len(filteredVals) > 0 {
			values[k] = filteredVals
			keys = append(keys, k)
		}
	}

	if len(keys) == 0 {
		return ""
	}
	slices.Sort(keys)

	qsBuf := getBuffer()
	defer putBuffer(qsBuf)

	first := true
	for _, k := range keys {
		vals := values[k]
		slices.Sort(vals)
		for _, v := range vals {
			if !first {
				qsBuf.WriteByte('&')
			}
			qsBuf.WriteString(url.QueryEscape(k))
			qsBuf.WriteByte('=')
			qsBuf.WriteString(url.QueryEscape(v))
			first = false
		}
	}

	return qsBuf.String()
}

// buildUnsortedQueryString filters query params while preserving original ordering.
func buildUnsortedQueryString(r *http.Request, cfg *CacheKey) string {
	qsBuf := getBuffer()
	defer putBuffer(qsBuf)

	first := true
	for part := range strings.SplitSeq(r.URL.RawQuery, "&") {
		if part == "" {
			continue
		}
		rawKey, rawVal, hasVal := strings.Cut(part, "=")
		k, err := url.QueryUnescape(rawKey)
		if err != nil {
			k = rawKey
		}

		v := ""
		if hasVal {
			v, err = url.QueryUnescape(rawVal)
			if err != nil {
				v = rawVal
			}
		}

		if !isQueryParamAllowed(k, v, cfg) {
			continue
		}

		if !first {
			qsBuf.WriteByte('&')
		}
		qsBuf.WriteString(url.QueryEscape(k))
		qsBuf.WriteByte('=')
		qsBuf.WriteString(url.QueryEscape(v))
		first = false
	}

	return qsBuf.String()
}

// isSortableVaryHeader reports whether name is a standard content-negotiation header
// whose comma-separated tokens can be sorted deterministically without changing semantics.
func isSortableVaryHeader(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "accept-encoding", "accept-language", "accept":
		return true
	default:
		return false
	}
}

// generateVariantKey generates a deterministic variant key based on matched Vary request headers.
func generateVariantKey(r *http.Request, varyHeaderNames []string) string {
	if len(varyHeaderNames) == 0 {
		return ""
	}

	headers := slices.Clone(varyHeaderNames)
	slices.Sort(headers)

	buf := getBuffer()
	defer putBuffer(buf)

	first := true
	for _, name := range headers {
		canonicalName := strings.ToLower(strings.TrimSpace(name))
		if canonicalName == "" {
			continue
		}

		vals := r.Header.Values(canonicalName)

		if !first {
			buf.WriteByte('|')
		}
		buf.WriteString(canonicalName)
		buf.WriteByte('=')
		if len(vals) > 0 {
			if isSortableVaryHeader(canonicalName) {
				var tokens []string
				for _, v := range vals {
					for tok := range strings.SplitSeq(v, ",") {
						trimmed := strings.TrimSpace(tok)
						if trimmed != "" {
							tokens = append(tokens, trimmed)
						}
					}
				}
				slices.Sort(tokens)
				for i, tok := range tokens {
					if i > 0 {
						buf.WriteByte(',')
					}
					buf.WriteString(tok)
				}
			} else {
				sortedVals := slices.Clone(vals)
				slices.Sort(sortedVals)
				for i, v := range sortedVals {
					if i > 0 {
						buf.WriteByte(',')
					}
					buf.WriteString(strings.TrimSpace(v))
				}
			}
		}
		first = false
	}

	return buf.String()
}

// normalizeHost lowercases the host and strips default ports (:80 for http, :443 for https).
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
