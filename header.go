package titip

// Standard and RFC HTTP Header constants used throughout Titip.
const (
	// Request / Response Caching & Freshness (RFC-7234, RFC-9211)
	HeaderCacheControl    = "Cache-Control"
	HeaderCacheStatus     = "Cache-Status"
	HeaderAge             = "Age"
	HeaderExpires         = "Expires"
	HeaderDate            = "Date"
	HeaderVary            = "Vary"
	HeaderETag            = "ETag"
	HeaderLastModified    = "Last-Modified"

	// Conditional Revalidation (RFC-7232)
	HeaderIfNoneMatch     = "If-None-Match"
	HeaderIfModifiedSince = "If-Modified-Since"

	// Protocol & Bypass Guards
	HeaderUpgrade         = "Upgrade"
	HeaderAccept          = "Accept"
	HeaderContentType     = "Content-Type"
	HeaderRange           = "Range"
	HeaderSetCookie       = "Set-Cookie"
	HeaderXForwardedProto = "X-Forwarded-Proto"

	// Surrogate & Tag Invalidation
	HeaderCacheTag        = "Cache-Tag"
	HeaderSurrogateKey    = "Surrogate-Key"

	// Common Header Values
	ContentTypeEventStream = "text/event-stream"
	UpgradeWebSocket       = "websocket"
)
