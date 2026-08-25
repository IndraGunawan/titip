package titip

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

	// Conditional Revalidation (RFC-7232)
	headerIfNoneMatch     = "If-None-Match"
	headerIfModifiedSince = "If-Modified-Since"

	// Protocol & Bypass Guards
	headerUpgrade         = "Upgrade"
	headerAccept          = "Accept"
	headerContentType     = "Content-Type"
	headerRange           = "Range"
	headerSetCookie       = "Set-Cookie"
	headerXForwardedProto = "X-Forwarded-Proto"

	// Surrogate & Tag Invalidation
	headerCacheTag = "Cache-Tag"

	// Common Header Values
	contentTypeEventStream = "text/event-stream"
	upgradeWebSocket       = "websocket"
)
