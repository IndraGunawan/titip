package titip

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pquerna/cachecontrol/cacheobject"
)

// MaxCacheTTL is the maximum allowed cache TTL (1 year) per RFC-7234 and Titip architecture constraints.
const MaxCacheTTL = 365 * 24 * time.Hour

// DefaultCacheableStatusCodes contains the standard standard set of cacheable HTTP status codes.
var DefaultCacheableStatusCodes = map[int]struct{}{
	http.StatusOK:                   {}, // 200
	http.StatusNonAuthoritativeInfo: {}, // 203
	http.StatusNoContent:            {}, // 204
	http.StatusPartialContent:       {}, // 206
	http.StatusMultipleChoices:      {}, // 300
	http.StatusMovedPermanently:     {}, // 301
	http.StatusFound:                {}, // 302
	http.StatusTemporaryRedirect:    {}, // 307
	http.StatusPermanentRedirect:    {}, // 308
	http.StatusBadRequest:           {}, // 400
	http.StatusForbidden:            {}, // 403
	http.StatusNotFound:             {}, // 404
	http.StatusMethodNotAllowed:     {}, // 405
	http.StatusGone:                 {}, // 410
	http.StatusUnavailableForLegalReasons: {}, // 451
	http.StatusInternalServerError:        {}, // 500
	http.StatusNotImplemented:             {}, // 501
	http.StatusBadGateway:                 {}, // 502
	http.StatusServiceUnavailable:         {}, // 503
	http.StatusGatewayTimeout:             {}, // 504
}

// FreshnessInfo encapsulates the calculated RFC-7234 Section 4.2.3 freshness metrics.
type FreshnessInfo struct {
	ApparentAge             time.Duration
	CorrectedInitialAge     time.Duration
	CurrentAge              time.Duration
	EffectiveTTL            time.Duration
	FreshnessLifetime       time.Duration
	StaleWhileRevalidateTTL time.Duration
	StaleIfErrorTTL         time.Duration
	IsCacheable             bool
	Directives              *cacheobject.ResponseCacheDirectives
}

// ParseAge parses the HTTP Age response header (delta-seconds).
func ParseAge(ageHeader string) time.Duration {
	trimmed := strings.TrimSpace(ageHeader)
	if trimmed == "" {
		return 0
	}
	seconds, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

// httpDateFormats contains standard RFC-7231 Section 7.1.1.1 date formats.
var httpDateFormats = []string{
	http.TimeFormat,               // "Mon, 02 Jan 2006 15:04:05 GMT" (RFC 1123 / RFC 7231)
	"Mon, 02 Jan 2006 15:04:05 -0700",
	time.RFC850,                  // "Monday, 02-Jan-06 15:04:05 GMT"
	time.ANSIC,                   // "Mon Jan _2 15:04:05 2006"
}

// ParseDate parses an HTTP date string into time.Time.
func ParseDate(dateHeader string) (time.Time, error) {
	trimmed := strings.TrimSpace(dateHeader)
	if trimmed == "" {
		return time.Time{}, nil
	}
	for _, format := range httpDateFormats {
		if t, err := time.Parse(format, trimmed); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, http.ErrServerClosed
}

// CalculateFreshness computes RFC-7234 Section 4.2.3 age and freshness values.
func CalculateFreshness(
	statusCode int,
	respHeaders http.Header,
	reqTime, respTime, now time.Time,
	customCacheableStatuses map[int]struct{},
) FreshnessInfo {
	if customCacheableStatuses == nil {
		customCacheableStatuses = DefaultCacheableStatusCodes
	}

	info := FreshnessInfo{}

	ccHeader := respHeaders.Get(HeaderCacheControl)
	if ccHeader != "" {
		if d, err := cacheobject.ParseResponseCacheControl(ccHeader); err == nil {
			info.Directives = d
		}
	}

	// RFC-7234 §4.2.3 Age Calculations
	dateVal, _ := ParseDate(respHeaders.Get(HeaderDate))
	ageVal := ParseAge(respHeaders.Get(HeaderAge))

	// 1. apparent_age = max(0, response_time - date_value)
	if !dateVal.IsZero() && respTime.After(dateVal) {
		info.ApparentAge = respTime.Sub(dateVal)
	}

	// 2. response_delay = response_time - request_time
	// corrected_age_value = age_value + response_delay
	responseDelay := time.Duration(0)
	if respTime.After(reqTime) {
		responseDelay = respTime.Sub(reqTime)
	}
	correctedAgeValue := ageVal + responseDelay

	// corrected_initial_age = max(apparent_age, corrected_age_value)
	if info.ApparentAge > correctedAgeValue {
		info.CorrectedInitialAge = info.ApparentAge
	} else {
		info.CorrectedInitialAge = correctedAgeValue
	}

	// 3. resident_time = now - response_time
	residentTime := time.Duration(0)
	if now.After(respTime) {
		residentTime = now.Sub(respTime)
	}

	// current_age = corrected_initial_age + resident_time
	info.CurrentAge = info.CorrectedInitialAge + residentTime

	// 4. Freshness Lifetime
	if info.Directives != nil {
		if info.Directives.SMaxAge >= 0 {
			info.FreshnessLifetime = time.Duration(info.Directives.SMaxAge) * time.Second
		} else if info.Directives.MaxAge >= 0 {
			info.FreshnessLifetime = time.Duration(info.Directives.MaxAge) * time.Second
		} else if expHeader := respHeaders.Get(HeaderExpires); expHeader != "" {
			expDate, err := ParseDate(expHeader)
			if err == nil && !expDate.IsZero() && !dateVal.IsZero() && expDate.After(dateVal) {
				info.FreshnessLifetime = expDate.Sub(dateVal)
			}
		}

		if info.Directives.StaleWhileRevalidate > 0 {
			info.StaleWhileRevalidateTTL = time.Duration(info.Directives.StaleWhileRevalidate) * time.Second
		}
		if info.Directives.StaleIfError > 0 {
			info.StaleIfErrorTTL = time.Duration(info.Directives.StaleIfError) * time.Second
		}
	}

	// 5. Effective TTL stored in cache
	if info.FreshnessLifetime > info.CorrectedInitialAge {
		info.EffectiveTTL = info.FreshnessLifetime - info.CorrectedInitialAge
	} else {
		info.EffectiveTTL = 0
	}

	// Clamp to 1 year max TTL
	if info.EffectiveTTL > MaxCacheTTL {
		info.EffectiveTTL = MaxCacheTTL
	}

	// Determine cacheability
	info.IsCacheable = IsResponseCacheable(statusCode, info.Directives, respHeaders, customCacheableStatuses)

	return info
}

// IsResponseCacheable determines if a response is strictly cacheable under RFC-7234 & Titip policies.
func IsResponseCacheable(
	statusCode int,
	directives *cacheobject.ResponseCacheDirectives,
	respHeaders http.Header,
	allowedStatuses map[int]struct{},
) bool {
	if allowedStatuses == nil {
		allowedStatuses = DefaultCacheableStatusCodes
	}

	// Must be in cacheable status code set
	if _, ok := allowedStatuses[statusCode]; !ok {
		return false
	}

	// Prohibit caching if Set-Cookie header is present (NEVER leak user sessions)
	if respHeaders.Get(HeaderSetCookie) != "" {
		return false
	}

	// Must have explicit Cache-Control (no heuristic caching)
	if directives == nil {
		return false
	}

	// Prohibit no-store
	if directives.NoStore {
		return false
	}

	// Prohibit private in shared cache
	if directives.PrivatePresent || len(directives.Private) > 0 {
		return false
	}

	// Must have explicit freshness indicator (max-age, s-maxage, or public)
	if directives.MaxAge < 0 && directives.SMaxAge < 0 && !directives.Public {
		return false
	}

	return true
}
