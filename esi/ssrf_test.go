package esi

import (
	"context"
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
		"/cart?id=123",
		"http://example.com/api",
		"https://example.com/api",
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
	}

	for _, u := range invalid {
		if _, err := ValidateURLScheme(u); err == nil {
			t.Errorf("expected URL %q to be rejected, but got valid", u)
		}
	}
}

func TestSSRF_MatchHost(t *testing.T) {
	patterns := []string{"cdn.example.com", "*.partner.com", "api.service.io:8080"}

	tests := []struct {
		host  string
		match bool
	}{
		{"cdn.example.com", true},
		{"cdn.example.com:443", true},
		{"sub.partner.com", true},
		{"sub.partner.com:8443", true},
		{"partner.com", true},
		{"otherpartner.com", false},
		{"attacker.com", false},
		{"api.service.io:8080", true},
		{"api.service.io", true},
	}

	for _, tt := range tests {
		got := MatchHost(tt.host, patterns)
		if got != tt.match {
			t.Errorf("MatchHost(%q, %v) = %v; want %v", tt.host, patterns, got, tt.match)
		}
	}
}

func TestSSRF_TransportDialBlocked(t *testing.T) {
	// Start local test server on 127.0.0.1
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
