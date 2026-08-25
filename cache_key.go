package titip

import (
	"net/http"
	"net/url"
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

// KeyConfig defines the configuration for assembling zero-hash canonical cache keys.
type KeyConfig struct {
	// IncludeProtocol includes the request scheme ("http://" or "https://") in the cache key.
	// When true, HTTP and HTTPS requests reference distinct cache entries.
	IncludeProtocol bool

	// ExcludeHost excludes the HTTP Host / domain from the cache key.
	// When true, Host is omitted so multiple domains serving identical content share cache entries.
	ExcludeHost bool

	// KeepTrailingSlash preserves trailing slashes in the URL path.
	// When true, exact trailing slashes are preserved as distinct paths.
	KeepTrailingSlash bool

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
func generatePrimaryKey(r *http.Request, cfg *KeyConfig) string {
	if cfg == nil {
		cfg = &KeyConfig{}
	}

	buf := getBuffer()
	defer putBuffer(buf)

	if cfg.IncludeProtocol {
		if r.TLS != nil || r.Header.Get(headerXForwardedProto) == "https" || (r.URL != nil && r.URL.Scheme == "https") {
			buf.WriteString("https://")
		} else {
			buf.WriteString("http://")
		}
	}

	if !cfg.ExcludeHost {
		host := r.Host
		if host == "" && r.URL != nil {
			host = r.URL.Host
		}
		buf.WriteString(strings.ToLower(host))
	}

	path := "/"
	if r.URL != nil && r.URL.Path != "" {
		path = r.URL.Path
	}
	if !cfg.KeepTrailingSlash && len(path) > 1 && strings.HasSuffix(path, "/") {
		path = strings.TrimRight(path, "/")
	}
	buf.WriteString(path)

	// Query parameter handling
	if !cfg.ExcludeQueryString && r.URL != nil && r.URL.RawQuery != "" {
		if cfg.DisableQueryStringSort {
			firstParam := true
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
					var err error
					v, err = url.QueryUnescape(rawVal)
					if err != nil {
						v = rawVal
					}
				}

				if firstParam {
					buf.WriteByte('?')
					firstParam = false
				} else {
					buf.WriteByte('&')
				}
				buf.WriteString(url.QueryEscape(k))
				buf.WriteByte('=')
				buf.WriteString(url.QueryEscape(v))
			}
		} else {
			if values, err := url.ParseQuery(r.URL.RawQuery); err == nil && len(values) > 0 {
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

				if len(keys) > 0 {
					slices.Sort(keys)
					buf.WriteByte('?')
					firstParam := true
					for _, k := range keys {
						vals := values[k]
						slices.Sort(vals)
						for _, v := range vals {
							if !firstParam {
								buf.WriteByte('&')
							}
							buf.WriteString(url.QueryEscape(k))
							buf.WriteByte('=')
							buf.WriteString(url.QueryEscape(v))
							firstParam = false
						}
					}
				}
			}
		}
	}

	// Request header values inclusion
	if len(cfg.IncludedHeaderNames) > 0 {
		headers := slices.Clone(cfg.IncludedHeaderNames)
		slices.Sort(headers)
		for _, h := range headers {
			hLower := strings.ToLower(h)
			vals := r.Header.Values(h)
			if len(vals) > 0 {
				buf.WriteString("|h:")
				buf.WriteString(hLower)
				buf.WriteByte('=')
				for i, v := range vals {
					if i > 0 {
						buf.WriteByte(',')
					}
					buf.WriteString(strings.TrimSpace(v))
				}
			}
		}
	}

	// Cookie values inclusion
	if len(cfg.IncludedCookieNames) > 0 {
		cookieNames := slices.Clone(cfg.IncludedCookieNames)
		slices.Sort(cookieNames)
		for _, name := range cookieNames {
			if cookie, err := r.Cookie(name); err == nil && cookie != nil && cookie.Value != "" {
				buf.WriteString("|c:")
				buf.WriteString(name)
				buf.WriteByte('=')
				buf.WriteString(cookie.Value)
			}
		}
	}

	return buf.String()
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
