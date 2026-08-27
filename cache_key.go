package titip

import (
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

// defaultPorts maps scheme to its default port string for stripping from host.
var defaultPorts = map[string]string{
	"http":  ":80",
	"https": ":443",
}

// KeyConfig defines the configuration for assembling zero-hash canonical cache keys.
type KeyConfig struct {
	// IncludeProtocol includes the request scheme ("http" or "https") in the cache key as s=<scheme>.
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

	// IncludedQueryParams specifies a whitelist of query parameter names to include in the cache key.
	// If set, only these specific parameters are included in the cache key.
	IncludedQueryParams []string

	// ExcludedQueryParams specifies a blacklist of query parameter names to exclude from the cache key.
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
}

// generatePrimaryKey constructs a canonical, zero-hash primary cache key for a request.
//
// Format: meta:p=<path>:h=<host>:m=<method>[:s=<scheme>][:qs=<query>][:he=<headers>][:ck=<cookies>]
//
// Component ordering is fixed: path → host → method → scheme → query → headers → cookies.
// All component values are percent-encoded where they contain delimiter characters (:, =).
func generatePrimaryKey(r *http.Request, cfg *KeyConfig) string {
	if cfg == nil {
		cfg = &KeyConfig{}
	}

	buf := getBuffer()
	defer putBuffer(buf)

	// --- p=<path> (always first) ---
	rawPath := "/"
	if r.URL != nil && r.URL.Path != "" {
		rawPath = r.URL.EscapedPath()
		if rawPath == "" {
			rawPath = r.URL.Path
		}
	}
	// Clean the path (resolves ../, ./, double-slashes and normalizes trailing slashes).
	cleanedPath := path.Clean(rawPath)
	if cleanedPath == "." {
		cleanedPath = "/"
	}

	buf.WriteString("p=")
	buf.WriteString(cleanedPath)

	// --- h=<host> (always second, unless excluded) ---
	if !cfg.ExcludeHost {
		host := r.Host
		if host == "" && r.URL != nil {
			host = r.URL.Host
		}
		host = strings.ToLower(host)

		// Strip default ports (:80 for http, :443 for https).
		scheme := resolveScheme(r)
		if port, ok := defaultPorts[scheme]; ok {
			host = strings.TrimSuffix(host, port)
		}

		if host != "" {
			buf.WriteString(":h=")
			buf.WriteString(host)
		}
	}

	// --- m=<method> (always present; HEAD normalises to GET) ---
	method := r.Method
	if method == http.MethodHead || method == "" {
		method = http.MethodGet
	}
	buf.WriteString(":m=")
	buf.WriteString(method)

	// --- s=<scheme> (optional, only when IncludeProtocol == true) ---
	if cfg.IncludeProtocol {
		buf.WriteString(":s=")
		buf.WriteString(resolveScheme(r))
	}

	// --- qs=<query> (optional, filtered and sorted) ---
	if !cfg.ExcludeQueryString && r.URL != nil && r.URL.RawQuery != "" {
		qs := buildQueryString(r, cfg)
		if qs != "" {
			buf.WriteString(":qs=")
			buf.WriteString(qs)
		}
	}

	// --- he=<headers> (optional, percent-encoded values to prevent delimiter injection) ---
	if len(cfg.IncludedHeaderNames) > 0 {
		headers := slices.Clone(cfg.IncludedHeaderNames)
		slices.Sort(headers)
		for _, h := range headers {
			hLower := strings.ToLower(h)
			vals := r.Header.Values(h)
			if len(vals) == 0 {
				continue
			}
			buf.WriteString(":he=")
			buf.WriteString(hLower)
			buf.WriteByte('~')
			for i, v := range vals {
				if i > 0 {
					buf.WriteByte(',')
				}
				// Percent-encode values to prevent : and = from colliding with key delimiters.
				buf.WriteString(url.QueryEscape(strings.TrimSpace(v)))
			}
		}
	}

	// --- ck=<cookies> (optional, percent-encoded values) ---
	if len(cfg.IncludedCookieNames) > 0 {
		cookieNames := slices.Clone(cfg.IncludedCookieNames)
		slices.Sort(cookieNames)
		for _, name := range cookieNames {
			cookie, err := r.Cookie(name)
			if err != nil || cookie == nil || cookie.Value == "" {
				continue
			}
			buf.WriteString(":ck=")
			buf.WriteString(name)
			buf.WriteByte('~')
			buf.WriteString(url.QueryEscape(cookie.Value))
		}
	}

	return buf.String()
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
func buildQueryString(r *http.Request, cfg *KeyConfig) string {
	if cfg.DisableQueryStringSort {
		return buildUnsortedQueryString(r, cfg)
	}
	return buildSortedQueryString(r, cfg)
}

// buildSortedQueryString parses, filters, sorts, and reassembles the query string.
func buildSortedQueryString(r *http.Request, cfg *KeyConfig) string {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(values) == 0 {
		return ""
	}

	keys := make([]string, 0, len(values))
	if len(cfg.IncludedQueryParams) > 0 {
		for k := range values {
			if slices.Contains(cfg.IncludedQueryParams, k) {
				keys = append(keys, k)
			}
		}
	} else {
		for k := range values {
			if slices.Contains(cfg.ExcludedQueryParams, k) {
				continue
			}
			if cfg.ExcludeMarketingParams && slices.Contains(defaultMarketingQueryParams, k) {
				continue
			}
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
func buildUnsortedQueryString(r *http.Request, cfg *KeyConfig) string {
	qsBuf := getBuffer()
	defer putBuffer(qsBuf)

	first := true
	for _, part := range strings.Split(r.URL.RawQuery, "&") {
		if part == "" {
			continue
		}
		rawKey, rawVal, hasVal := strings.Cut(part, "=")
		k, err := url.QueryUnescape(rawKey)
		if err != nil {
			k = rawKey
		}

		if len(cfg.IncludedQueryParams) > 0 {
			if !slices.Contains(cfg.IncludedQueryParams, k) {
				continue
			}
		} else {
			if slices.Contains(cfg.ExcludedQueryParams, k) {
				continue
			}
			if cfg.ExcludeMarketingParams && slices.Contains(defaultMarketingQueryParams, k) {
				continue
			}
		}

		v := ""
		if hasVal {
			v, err = url.QueryUnescape(rawVal)
			if err != nil {
				v = rawVal
			}
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
		if len(vals) == 0 {
			// Also check standard Header get
			val := r.Header.Get(canonicalName)
			if val != "" {
				vals = []string{val}
			}
		}

		if !first {
			buf.WriteByte('|')
		}
		buf.WriteString(canonicalName)
		buf.WriteByte('=')
		if len(vals) > 0 {
			sortedVals := slices.Clone(vals)
			slices.Sort(sortedVals)
			for i, v := range sortedVals {
				if i > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(strings.ToLower(strings.TrimSpace(v)))
			}
		}
		first = false
	}

	return buf.String()
}
