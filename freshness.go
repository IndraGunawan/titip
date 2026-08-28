package titip

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pquerna/cachecontrol/cacheobject"
)

// maxCacheTTL is the maximum allowed cache TTL (1 year) per RFC 9111 / RFC 7234 and Titip architecture constraints.
const maxCacheTTL = 365 * 24 * time.Hour

// defaultCacheableStatusCodes contains the standard standard set of cacheable HTTP status codes.
var defaultCacheableStatusCodes = map[int]struct{}{
	http.StatusOK:                         {}, // 200
	http.StatusNonAuthoritativeInfo:       {}, // 203
	http.StatusNoContent:                  {}, // 204
	http.StatusPartialContent:             {}, // 206
	http.StatusMultipleChoices:            {}, // 300
	http.StatusMovedPermanently:           {}, // 301
	http.StatusFound:                      {}, // 302
	http.StatusTemporaryRedirect:          {}, // 307
	http.StatusPermanentRedirect:          {}, // 308
	http.StatusBadRequest:                 {}, // 400
	http.StatusForbidden:                  {}, // 403
	http.StatusNotFound:                   {}, // 404
	http.StatusMethodNotAllowed:           {}, // 405
	http.StatusGone:                       {}, // 410
	http.StatusRequestURITooLong:          {}, // 414
	http.StatusUnavailableForLegalReasons: {}, // 451
	http.StatusInternalServerError:        {}, // 500
	http.StatusNotImplemented:             {}, // 501
	http.StatusBadGateway:                 {}, // 502
	http.StatusServiceUnavailable:         {}, // 503
	http.StatusGatewayTimeout:             {}, // 504
}

// freshnessInfo encapsulates the calculated RFC 9111 (and RFC 7234 Section 4.2.3) freshness metrics.
type freshnessInfo struct {
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

// parseAge parses the HTTP Age response header (delta-seconds).
func parseAge(ageHeader string) time.Duration {
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
	http.TimeFormat, // "Mon, 02 Jan 2006 15:04:05 GMT" (RFC 1123 / RFC 7231)
	"Mon, 02 Jan 2006 15:04:05 -0700",
	time.RFC850, // "Monday, 02-Jan-06 15:04:05 GMT"
	time.ANSIC,  // "Mon Jan _2 15:04:05 2006"
}

// parseDate parses an HTTP date string into time.Time.
func parseDate(dateHeader string) (time.Time, error) {
	trimmed := strings.TrimSpace(dateHeader)
	if trimmed == "" || trimmed == "0" || trimmed == "-1" {
		return time.Time{}, nil
	}
	for _, format := range httpDateFormats {
		if t, err := time.Parse(format, trimmed); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, http.ErrServerClosed
}

// calculateFreshness computes RFC 9111 (and RFC 7234 Section 4.2.3) age and freshness values.
func calculateFreshness(
	statusCode int,
	reqHeaders, respHeaders http.Header,
	reqTime, respTime, now time.Time,
) freshnessInfo {
	info := freshnessInfo{}

	ccHeader := respHeaders.Get(headerCacheControl)
	if ccHeader != "" {
		if d, err := cacheobject.ParseResponseCacheControl(ccHeader); err == nil {
			info.Directives = d
		}
	}

	// RFC 9111 §4.2.3 & RFC 7234 §4.2.3: Calculating Age
	dateVal, _ := parseDate(respHeaders.Get(headerDate))
	ageVal := parseAge(respHeaders.Get(headerAge))

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
	info.CorrectedInitialAge = max(info.ApparentAge, correctedAgeValue)

	// 3. resident_time = now - response_time
	residentTime := time.Duration(0)
	if now.After(respTime) {
		residentTime = now.Sub(respTime)
	}

	info.CurrentAge = info.CorrectedInitialAge + residentTime

	// 4. RFC 9111 §4.2.1: Freshness Lifetime calculation
	if info.Directives != nil {
		// RFC 9111 §5.2.2.10: s-maxage overrides max-age for shared caches
		if info.Directives.SMaxAge >= 0 {
			info.FreshnessLifetime = time.Duration(info.Directives.SMaxAge) * time.Second
		} else if info.Directives.MaxAge >= 0 { // RFC 9111 §5.2.2.1: max-age directive
			info.FreshnessLifetime = time.Duration(info.Directives.MaxAge) * time.Second
		} else if expHeader := respHeaders.Get(headerExpires); expHeader != "" { // RFC 9111 §5.3: Expires header
			expDate, err := parseDate(expHeader)
			if err == nil && !expDate.IsZero() {
				if !dateVal.IsZero() && expDate.After(dateVal) {
					info.FreshnessLifetime = expDate.Sub(dateVal)
				} else if expDate.After(respTime) {
					info.FreshnessLifetime = expDate.Sub(respTime)
				}
			}
		}

		// RFC 9111 §5.2.2.4: no-cache requires revalidation before each use (FreshnessLifetime = 0)
		if info.Directives.NoCachePresent || len(info.Directives.NoCache) > 0 {
			info.FreshnessLifetime = 0
		}

		// RFC 5861 §3 & §4 / RFC 9111 §5.2.2.1: must-revalidate and proxy-revalidate forbid serving stale
		if !info.Directives.MustRevalidate && !info.Directives.ProxyRevalidate {
			if info.Directives.StaleWhileRevalidate > 0 {
				info.StaleWhileRevalidateTTL = time.Duration(info.Directives.StaleWhileRevalidate) * time.Second
			}
			if info.Directives.StaleIfError > 0 {
				info.StaleIfErrorTTL = time.Duration(info.Directives.StaleIfError) * time.Second
			}
		}
	} else if expHeader := respHeaders.Get(headerExpires); expHeader != "" {
		// RFC 9111 §4.2.1: Expires without Cache-Control provides freshness lifetime
		expDate, err := parseDate(expHeader)
		if err == nil && !expDate.IsZero() {
			if !dateVal.IsZero() && expDate.After(dateVal) {
				info.FreshnessLifetime = expDate.Sub(dateVal)
			} else if expDate.After(respTime) {
				info.FreshnessLifetime = expDate.Sub(respTime)
			}
		}
	}

	// 5. Effective TTL stored in cache (freshness_lifetime - corrected_initial_age)
	if info.FreshnessLifetime > info.CorrectedInitialAge {
		info.EffectiveTTL = info.FreshnessLifetime - info.CorrectedInitialAge
	} else {
		info.EffectiveTTL = 0
	}

	// Clamp to 1 year max TTL
	if info.EffectiveTTL > maxCacheTTL {
		info.EffectiveTTL = maxCacheTTL
	}

	// Determine cacheability
	info.IsCacheable = isResponseCacheable(statusCode, reqHeaders, respHeaders, info.Directives)

	return info
}

// isResponseCacheable determines if a response is cacheable under RFC 9111 & Titip policies.
func isResponseCacheable(
	statusCode int,
	reqHeaders, respHeaders http.Header,
	directives *cacheobject.ResponseCacheDirectives,
) bool {
	// RFC 9110 §15 / RFC 9111 §3.1: 1xx Informational, 205 Reset Content, and 206 Partial Content are uncacheable
	if statusCode < 200 || statusCode == http.StatusResetContent || statusCode == http.StatusPartialContent {
		return false
	}

	// Prohibit caching if Set-Cookie header is present (NEVER leak user sessions per AGENTS.md)
	if respHeaders.Get(headerSetCookie) != "" {
		return false
	}

	// Prohibit caching SSE event-stream responses
	if strings.Contains(strings.ToLower(respHeaders.Get(headerContentType)), contentTypeEventStream) {
		return false
	}

	// RFC 9111 §4.1 / RFC 7231 §7.1.4: Vary: * prohibits shared caching and subsequent matching
	if vary := respHeaders.Get(headerVary); vary != "" {
		for _, v := range strings.Split(vary, ",") {
			if strings.TrimSpace(v) == "*" {
				return false
			}
		}
	}

	// RFC 9111 §4.2.1: If no Cache-Control header, check if Expires header provides future freshness
	if directives == nil {
		expHeader := respHeaders.Get(headerExpires)
		if expHeader == "" {
			return false
		}
		expDate, err := parseDate(expHeader)
		if err != nil || expDate.IsZero() || !expDate.After(time.Now()) {
			return false
		}
		// RFC 9111 §3.5: Request Authorization header guard for shared cache
		if reqHeaders != nil && reqHeaders.Get(headerAuthorization) != "" {
			return false
		}
		return true
	}

	// RFC 9111 §5.2.2.5: no-store response directive prohibits caching
	if directives.NoStore {
		return false
	}

	// RFC 9111 §5.2.2.7: private response directive prohibits shared caching
	if directives.PrivatePresent || len(directives.Private) > 0 {
		return false
	}

	// RFC 9111 §3.5: Shared cache MUST NOT store response to request containing Authorization
	// unless response contains public, s-maxage, or must-revalidate
	if reqHeaders != nil && reqHeaders.Get(headerAuthorization) != "" {
		if !directives.Public && directives.SMaxAge < 0 && !directives.MustRevalidate {
			return false
		}
	}

	// RFC 9111 §4.2.1 & §5.2.2: Must have explicit freshness indicator (max-age, s-maxage, public, no-cache, or valid Expires)
	if directives.MaxAge < 0 && directives.SMaxAge < 0 && !directives.Public && !directives.NoCachePresent && len(directives.NoCache) == 0 {
		expHeader := respHeaders.Get(headerExpires)
		if expHeader == "" {
			return false
		}
		expDate, err := parseDate(expHeader)
		if err != nil || expDate.IsZero() || !expDate.After(time.Now()) {
			return false
		}
	}

	return true
}
