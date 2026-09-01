package titip

import (
	"net/http"
	"strings"
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

// getHeaderValue retrieves the first value for a header key, with case-insensitive fallback.
func getHeaderValue(h http.Header, key string) string {
	if val := h.Get(key); val != "" {
		return val
	}
	for k, vv := range h {
		if strings.EqualFold(k, key) && len(vv) > 0 {
			return vv[0]
		}
	}
	return ""
}
