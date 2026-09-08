package esi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestSSRF_BlockedIPs(t *testing.T) {
	blockedCases := []string{
		"127.0.0.1",
		"10.0.1.50",
		"172.16.5.10",
		"192.168.1.1",
		"169.254.169.254",
		"0.0.0.0",
		"::1",
		"fe80::1",
		"fc00::1",
		// IPv4-mapped IPv6
		"::ffff:127.0.0.1",
		"::ffff:10.0.1.50",
		"::ffff:192.168.1.1",
		// Carrier-Grade NAT (RFC 6598)
		"100.64.0.1",
		// IETF Protocol Assignments
		"192.0.0.1",
		// Network Benchmark
		"198.18.0.1",
		// Multicast
		"224.0.0.1",
		// Reserved
		"240.0.0.1",
	}

	for _, ipStr := range blockedCases {
		ip := netip.MustParseAddr(ipStr)
		if !IsIPBlocked(ip) {
			t.Errorf("expected IP %s to be blocked", ipStr)
		}
	}

	allowedCases := []string{
		"8.8.8.8",
		"1.1.1.1",
		"142.250.190.46",
		"2606:4700:4700::1111",
		"::ffff:8.8.8.8", // IPv4-mapped public IP
	}

	for _, ipStr := range allowedCases {
		ip := netip.MustParseAddr(ipStr)
		if IsIPBlocked(ip) {
			t.Errorf("expected public IP %s to NOT be blocked", ipStr)
		}
	}
}

func TestSSRF_ValidateURLScheme(t *testing.T) {
	valid := []string{
		"/api/user",
		"/cart?id=123#checkout",
		"http://example.com/api",
		"https://example.com/api",
		"HTTP://EXAMPLE.COM/API",
		"HTTPS://EXAMPLE.COM/API",
	}

	for _, u := range valid {
		if _, err := ValidateURLScheme(u); err != nil {
			t.Errorf("expected URL %q to be valid, got: %v", u, err)
		}
	}

	invalid := []string{
		"file:///etc/passwd",
		"data:text/html,<html>",
		"javascript:alert(1)",
		"ftp://ftp.example.com",
		"gopher://evil.com",
		"",
		"http://[::1]:namedport/frag",
	}

	for _, u := range invalid {
		if _, err := ValidateURLScheme(u); err == nil {
			t.Errorf("expected URL %q to be rejected, but got valid", u)
		}
	}
}

func TestSSRF_MatchHost(t *testing.T) {
	patterns := []string{"cdn.example.com", "*.partner.com", "api.service.io:8080", "*"}

	tests := []struct {
		host     string
		patterns []string
		match    bool
	}{
		{"cdn.example.com", patterns[:3], true},
		{"cdn.example.com:443", patterns[:3], true},
		{"sub.partner.com", patterns[:3], true},
		{"sub.partner.com:8443", patterns[:3], true},
		{"partner.com", patterns[:3], true},
		{"otherpartner.com", patterns[:3], false},
		{"attacker.com", patterns[:3], false},
		{"api.service.io:8080", patterns[:3], true},
		{"api.service.io", patterns[:3], true},
		{"any.example.org", patterns, true}, // wildcard "*"
		{"anything", []string{}, true},      // empty pattern list allows all
		{"   ", []string{"cdn.example.com"}, false},
	}

	for _, tt := range tests {
		got := MatchHost(tt.host, tt.patterns)
		if got != tt.match {
			t.Errorf("MatchHost(%q, %v) = %v; want %v", tt.host, tt.patterns, got, tt.match)
		}
	}
}

func TestSSRF_TransportDialBlocked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cfg := SSRFConfig{
		BlockPrivateIPs: true,
	}
	tr := NewSSRFSafeTransport(cfg, 500*time.Millisecond)
	client := &http.Client{Transport: tr}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("failed to create req: %v", err)
	}

	_, err = client.Do(req)
	if err == nil {
		t.Fatalf("expected SSRF error when dialing 127.0.0.1 with BlockPrivateIPs=true, but succeeded")
	}
}

func TestSSRF_Transport_AllowPrivateIPs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("allowed-private"))
	}))
	defer srv.Close()

	// 1. BlockPrivateIPs = false allows dialing 127.0.0.1
	cfgDisabled := SSRFConfig{
		BlockPrivateIPs: false,
	}
	trDisabled := NewSSRFSafeTransport(cfgDisabled, 0) // verifies default timeout <= 0
	client := &http.Client{Transport: trDisabled}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("expected request to succeed when BlockPrivateIPs=false, got: %v", err)
	}
	_ = resp.Body.Close()

	// 2. AllowedHosts restriction rejection
	cfgRestricted := SSRFConfig{
		BlockPrivateIPs: false,
		AllowedHosts:    []string{"authorized-domain.com"},
	}
	trRestricted := NewSSRFSafeTransport(cfgRestricted, 500*time.Millisecond)
	clientRestricted := &http.Client{Transport: trRestricted}

	_, err = clientRestricted.Get(srv.URL)
	if err == nil || !errors.Is(err, ErrHostNotAllowed) {
		t.Fatalf("expected ErrHostNotAllowed, got: %v", err)
	}

	// 3. AllowPrivateIPsForAllowedHosts permits private IP when explicitly in AllowedHosts
	cfgAllowedPrivate := SSRFConfig{
		BlockPrivateIPs:                true,
		AllowedHosts:                   []string{"127.0.0.1"},
		AllowPrivateIPsForAllowedHosts: true,
	}
	trAllowedPrivate := NewSSRFSafeTransport(cfgAllowedPrivate, 500*time.Millisecond)
	clientAllowedPrivate := &http.Client{Transport: trAllowedPrivate}

	resp, err = clientAllowedPrivate.Get(srv.URL)
	if err != nil {
		t.Fatalf("expected request to succeed with AllowPrivateIPsForAllowedHosts=true, got: %v", err)
	}
	_ = resp.Body.Close()
}
