package titip

import (
	"net/http"
	"slices"
	"strings"
	"unicode"
)

// Standard and RFC HTTP Header constants used throughout Titip.
const (
	// Request / Response Caching & Freshness (RFC-7234, RFC-9111, RFC-9211, RFC-9213)
	headerCacheControl      = "Cache-Control"
	headerCDNCacheControl   = "CDN-Cache-Control"
	headerTitipCacheControl = "Titip-Cache-Control"
	headerCacheStatus       = "Cache-Status"
	headerAge               = "Age"
	headerExpires           = "Expires"
	headerDate              = "Date"
	headerVary              = "Vary"
	headerETag              = "ETag"
	headerLastModified      = "Last-Modified"

	// Conditional Revalidation (RFC-7232, RFC-9110)
	headerIfMatch           = "If-Match"
	headerIfUnmodifiedSince = "If-Unmodified-Since"
	headerIfNoneMatch       = "If-None-Match"
	headerIfModifiedSince   = "If-Modified-Since"

	// Protocol & Bypass Guards
	headerUpgrade         = "Upgrade"
	headerAccept          = "Accept"
	headerAcceptLanguage  = "Accept-Language"
	headerContentType     = "Content-Type"
	headerContentLength   = "Content-Length"
	headerRange           = "Range"
	headerSetCookie       = "Set-Cookie"
	headerCookie          = "Cookie"
	headerUserAgent       = "User-Agent"
	headerXForwardedProto = "X-Forwarded-Proto"
	headerAuthorization   = "Authorization"
	headerPragma          = "Pragma"
	headerLocation        = "Location"
	headerContentLocation = "Content-Location"

	// Surrogate & Tag Invalidation / ESI
	headerCacheTag         = "Cache-Tag"
	headerSurrogateControl = "Surrogate-Control"

	// Common Header Values
	contentTypeEventStream = "text/event-stream"
	upgradeWebSocket       = "websocket"
)

// containsToken checks if a comma-separated header value contains target (case-insensitive).
func containsToken(headerVal, target string) bool {
	if headerVal == "" {
		return false
	}
	for tok := range strings.SplitSeq(headerVal, ",") {
		if strings.EqualFold(strings.TrimSpace(tok), target) {
			return true
		}
	}
	return false
}

// getHeaderValues retrieves all values for a header key, supporting both canonical HTTP key lookup
// and fallback case-insensitive matching for raw struct literals (e.g. CDN-Cache-Control).
func getHeaderValues(h http.Header, key string) []string {
	if vals := h.Values(key); len(vals) > 0 {
		return vals
	}
	for k, vv := range h {
		if strings.EqualFold(k, key) {
			return vv
		}
	}
	return nil
}

// strongETagMatches performs strong comparison per RFC 9110 §13.1.1 (neither may be weak).
func strongETagMatches(clientETag, cachedETag string) bool {
	c := strings.TrimSpace(clientETag)
	s := strings.TrimSpace(cachedETag)
	if c == "" || s == "" {
		return false
	}
	if strings.HasPrefix(c, "W/") || strings.HasPrefix(c, "w/") ||
		strings.HasPrefix(s, "W/") || strings.HasPrefix(s, "w/") {
		return false
	}
	return c == s
}

// etagMatches performs weak ETag comparison per RFC-7232 Section 2.3.2.
func etagMatches(clientETag, cachedETag string) bool {
	c := strings.TrimSpace(clientETag)
	s := strings.TrimSpace(cachedETag)
	if c == "" || s == "" {
		return false
	}
	c = strings.TrimPrefix(strings.TrimPrefix(c, "W/"), "w/")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "W/"), "w/")
	return c == s
}

func extractTags(headers http.Header, tagName string) []string {
	if tagName == "" {
		tagName = headerCacheTag
	}
	val := headers.Get(tagName)
	if val == "" {
		return nil
	}
	return splitAndTrimTags(val)
}

func splitAndTrimTags(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
}

func extractVaryHeaderNames(headers http.Header) []string {
	var names []string
	for _, varyHeader := range headers.Values(headerVary) {
		for p := range strings.SplitSeq(varyHeader, ",") {
			name := strings.TrimSpace(p)
			if name != "" && !slices.Contains(names, name) {
				names = append(names, name)
			}
		}
	}
	return names
}
