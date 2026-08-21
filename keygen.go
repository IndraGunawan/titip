package titip

import (
	"bytes"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// QueryParamMode defines how query parameters are filtered when assembling cache keys.
type QueryParamMode int

const (
	// QueryParamsAll includes all query parameters in the cache key.
	QueryParamsAll QueryParamMode = iota
	// QueryParamsNone ignores all query parameters in the cache key.
	QueryParamsNone
	// QueryParamsWhitelist includes only specified parameters.
	QueryParamsWhitelist
	// QueryParamsBlacklist includes all query parameters except specified ones.
	QueryParamsBlacklist
)

// QueryParamsExcludeAll is an alias for QueryParamsNone.
const QueryParamsExcludeAll = QueryParamsNone

// DefaultMarketingQueryParams contains commonly used advertising and tracking query parameters.
var DefaultMarketingQueryParams = []string{
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

// KeyConfig defines the configuration for assembling zero-hash cache keys.
type KeyConfig struct {
	IncludeProtocol bool
	IncludeHost     bool
	IncludePath     bool
	QueryMode       QueryParamMode
	QueryWhitelist  []string
	QueryBlacklist  []string
	IncludeHeaders  []string
	IncludeCookies  []string
}

// DefaultKeyConfig returns the standard default key generation configuration.
func DefaultKeyConfig() *KeyConfig {
	return &KeyConfig{
		IncludeProtocol: true,
		IncludeHost:     true,
		IncludePath:     true,
		QueryMode:       QueryParamsAll,
	}
}

// WithIgnoredMarketingParams configures the KeyConfig to blacklist common marketing query parameters.
func (cfg *KeyConfig) WithIgnoredMarketingParams() *KeyConfig {
	if cfg.QueryMode != QueryParamsWhitelist && cfg.QueryMode != QueryParamsNone {
		cfg.QueryMode = QueryParamsBlacklist
	}
	for _, p := range DefaultMarketingQueryParams {
		if !slices.Contains(cfg.QueryBlacklist, p) {
			cfg.QueryBlacklist = append(cfg.QueryBlacklist, p)
		}
	}
	return cfg
}

// GeneratePrimaryKey constructs a canonical, zero-hash primary cache key for a request.
func GeneratePrimaryKey(r *http.Request, cfg *KeyConfig) string {
	if cfg == nil {
		cfg = DefaultKeyConfig()
	}

	buf := GetBuffer()
	defer PutBuffer(buf)

	if cfg.IncludeProtocol {
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" || (r.URL != nil && r.URL.Scheme == "https") {
			buf.WriteString("https://")
		} else {
			buf.WriteString("http://")
		}
	}

	if cfg.IncludeHost {
		host := r.Host
		if host == "" && r.URL != nil {
			host = r.URL.Host
		}
		buf.WriteString(strings.ToLower(host))
	}

	if cfg.IncludePath {
		path := "/"
		if r.URL != nil && r.URL.Path != "" {
			path = r.URL.Path
		}
		buf.WriteString(path)
	}

	// Query parameter handling
	if cfg.QueryMode != QueryParamsNone && r.URL != nil && r.URL.RawQuery != "" {
		appendFilteredQueryParams(buf, r.URL.RawQuery, cfg)
	}

	// Request header values inclusion
	if len(cfg.IncludeHeaders) > 0 {
		headers := slices.Clone(cfg.IncludeHeaders)
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
	if len(cfg.IncludeCookies) > 0 {
		cookieNames := slices.Clone(cfg.IncludeCookies)
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

// appendFilteredQueryParams parses, filters, sorts, and writes query parameters to the buffer.
func appendFilteredQueryParams(buf *bytes.Buffer, rawQuery string, cfg *KeyConfig) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil || len(values) == 0 {
		return
	}

	keys := make([]string, 0, len(values))
	for k := range values {
		switch cfg.QueryMode {
		case QueryParamsWhitelist:
			if slices.Contains(cfg.QueryWhitelist, k) {
				keys = append(keys, k)
			}
		case QueryParamsBlacklist:
			if !slices.Contains(cfg.QueryBlacklist, k) {
				keys = append(keys, k)
			}
		case QueryParamsAll:
			keys = append(keys, k)
		}
	}

	if len(keys) == 0 {
		return
	}

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

// GenerateVariantKey generates a deterministic variant key based on matched Vary request headers.
func GenerateVariantKey(r *http.Request, varyHeaderNames []string) string {
	if len(varyHeaderNames) == 0 {
		return ""
	}

	headers := slices.Clone(varyHeaderNames)
	slices.Sort(headers)

	buf := GetBuffer()
	defer PutBuffer(buf)

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
