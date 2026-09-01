package titip

import "strings"

// Standard and RFC HTTP Header constants used throughout Titip.
const (
	// Request / Response Caching & Freshness (RFC-7234, RFC-9211)
	headerCacheControl = "Cache-Control"
	headerCacheStatus  = "Cache-Status"
	headerAge          = "Age"
	headerExpires      = "Expires"
	headerDate         = "Date"
	headerVary         = "Vary"
	headerETag         = "ETag"
	headerLastModified = "Last-Modified"

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
