package esi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

var (
	// ErrSSRFBlocked is returned when an include URL resolves to a forbidden or private IP.
	ErrSSRFBlocked = errors.New("titip: esi: request blocked by ssrf protection")
	// ErrInvalidScheme is returned when an include URL has an unapproved scheme.
	ErrInvalidScheme = errors.New("titip: esi: invalid or dangerous url scheme")
	// ErrHostNotAllowed is returned when an include host is not in the allowed hosts whitelist.
	ErrHostNotAllowed = errors.New("titip: esi: host is not allowed")
	// ErrInvalidMethod is returned when a non-GET/HEAD method is attempted.
	ErrInvalidMethod = errors.New("titip: esi: only GET and HEAD methods are permitted")
)

// blockedCIDRs contains prefixes not covered by netip.IsPrivate/IsLoopback/etc.
// ponytail: 10/8, 172.16/12, 192.168/16 and 127/8 are already IsPrivate/IsLoopback — kept only non-stdlib ones.
var blockedCIDRs = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // Current network
	netip.MustParsePrefix("100.64.0.0/10"),   // Carrier-Grade NAT (RFC 6598)
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF Protocol Assignments
	netip.MustParsePrefix("198.18.0.0/15"),   // Network Benchmark
	netip.MustParsePrefix("224.0.0.0/4"),     // Multicast
	netip.MustParsePrefix("240.0.0.0/4"),     // Reserved
	netip.MustParsePrefix("::1/128"),         // IPv6 Loopback
	netip.MustParsePrefix("fc00::/7"),        // IPv6 Unique Local Address
}

// SSRFConfig configures outbound ESI fetch security.
type SSRFConfig struct {
	BlockPrivateIPs                bool
	AllowedHosts                   []string
	AllowPrivateIPsForAllowedHosts bool
}

// ValidateURLScheme checks if the target URL has a safe scheme (relative, http, https).
func ValidateURLScheme(rawURL string) (*url.URL, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("%w: empty url", ErrInvalidScheme)
	}

	// Relative path
	if strings.HasPrefix(rawURL, "/") {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidScheme, err)
		}
		return parsed, nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidScheme, err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("%w: scheme %q is not permitted", ErrInvalidScheme, parsed.Scheme)
	}

	return parsed, nil
}

// IsIPBlocked returns true if the IP is unroutable, loopback, private, link-local, or cloud metadata.
func IsIPBlocked(ip netip.Addr) bool {
	// Unmap IPv4-in-IPv6 addresses (e.g. ::ffff:127.0.0.1)
	ip = ip.Unmap()

	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsPrivate() {
		return true
	}

	for _, prefix := range blockedCIDRs {
		if prefix.Contains(ip) {
			return true
		}
	}

	return false
}

// MatchHost checks if the target host matches any allowed host patterns (e.g. "cdn.example.com" or "*.partner.com").
func MatchHost(host string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}

	// Strip port from host if present
	h := host
	if parsedHost, _, err := net.SplitHostPort(h); err == nil {
		h = parsedHost
	}
	h = strings.ToLower(strings.TrimSpace(h))

	for _, pattern := range patterns {
		p := strings.ToLower(strings.TrimSpace(pattern))
		if p == "" {
			continue
		}
		// Strip port from pattern if present
		if parsedP, _, err := net.SplitHostPort(p); err == nil {
			p = parsedP
		}
		if p == "*" || p == h {
			return true
		}
		if strings.HasPrefix(p, "*.") {
			suffix := p[1:] // e.g. ".partner.com"
			if strings.HasSuffix(h, suffix) && len(h) > len(suffix) {
				return true
			}
			// Also match exact root domain without wildcard (e.g. partner.com)
			if h == p[2:] {
				return true
			}
		}
	}

	return false
}

// NewSSRFSafeTransport constructs an http.RoundTripper that validates IPs at dial time via Control.
func NewSSRFSafeTransport(cfg SSRFConfig, dialTimeout time.Duration) http.RoundTripper {
	if dialTimeout <= 0 {
		dialTimeout = 10 * time.Second
	}

	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			if !cfg.BlockPrivateIPs {
				return nil
			}

			host, _, err := net.SplitHostPort(address)
			if err != nil {
				host = address
			}

			ip, err := netip.ParseAddr(host)
			if err != nil {
				// If not parsed as IP string, let dialer proceed or resolver handle it
				return nil
			}

			if IsIPBlocked(ip) {
				return fmt.Errorf("%w: connection to blocked IP %s forbidden", ErrSSRFBlocked, ip.String())
			}

			return nil
		},
	}

	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			// Validate allowed hosts if configured
			if len(cfg.AllowedHosts) > 0 {
				if !MatchHost(host, cfg.AllowedHosts) {
					return nil, fmt.Errorf("%w: %q", ErrHostNotAllowed, host)
				}
				if cfg.AllowPrivateIPsForAllowedHosts {
					// Direct dial without IP blocking for specifically whitelisted hosts
					rawDialer := &net.Dialer{Timeout: dialTimeout}
					return rawDialer.DialContext(ctx, network, addr)
				}
			}

			// ponytail: Control already blocks private IPs post-resolution; explicit LookupNetIP caused double DNS.
			return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
		},
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: dialTimeout,
	}
}
