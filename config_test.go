package titip

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/indragunawan/titip/esi"
	"github.com/indragunawan/titip/internal/teststore"
)

func TestConfig_Defaults(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	mw, err := New(store)
	if err != nil {
		t.Fatalf("unexpected error creating middleware: %v", err)
	}

	if mw.config.storageTimeout != 5*time.Second {
		t.Errorf("expected default storageTimeout 5s, got %v", mw.config.storageTimeout)
	}
	if mw.config.backgroundFetchTimeout != 125*time.Second {
		t.Errorf("expected default backgroundFetchTimeout 125s, got %v", mw.config.backgroundFetchTimeout)
	}
	if mw.config.tagHeaderName != "Cache-Tag" {
		t.Errorf("expected default tagHeaderName Cache-Tag, got %s", mw.config.tagHeaderName)
	}
	if mw.config.cacheStatusMode != CacheStatusSimpleToken {
		t.Errorf("expected default cacheStatusMode CacheStatusSimpleToken, got %v", mw.config.cacheStatusMode)
	}
	if !mw.config.convertHeadToGet {
		t.Errorf("expected default convertHeadToGet to be true")
	}
	if mw.config.respectClientCacheControl {
		t.Errorf("expected default respectClientCacheControl to be false")
	}
	if mw.config.logger == nil {
		t.Errorf("expected default logger to be non-nil")
	}
}

func TestNew_InvalidOption(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	tests := []struct {
		name        string
		opt         Option
		errContains string
	}{
		{
			name:        "StorageTimeout zero",
			opt:         WithStorageTimeout(0),
			errContains: "storage timeout must be positive",
		},
		{
			name:        "StorageTimeout negative",
			opt:         WithStorageTimeout(-1 * time.Second),
			errContains: "storage timeout must be positive",
		},
		{
			name:        "BackgroundFetchTimeout negative",
			opt:         WithBackgroundFetchTimeout(-1 * time.Second),
			errContains: "background fetch timeout cannot be negative",
		},
		{
			name:        "TagHeader empty",
			opt:         WithTagHeader(""),
			errContains: "tag header name cannot be empty",
		},
		{
			name:        "TagHeader whitespace only",
			opt:         WithTagHeader("   "),
			errContains: "tag header name cannot be empty",
		},
		{
			name:        "CacheStatusMode negative",
			opt:         WithCacheStatus(CacheStatusMode(-1)),
			errContains: "invalid cache status mode -1",
		},
		{
			name:        "CacheStatusMode above range",
			opt:         WithCacheStatus(CacheStatusMode(999)),
			errContains: "invalid cache status mode 999",
		},
		{
			name:        "ServerTimingCookie empty name",
			opt:         WithServerTimingCookie("", "secret"),
			errContains: "server timing cookie name and value cannot be empty",
		},
		{
			name:        "ServerTimingCookie whitespace name",
			opt:         WithServerTimingCookie("   ", "secret"),
			errContains: "server timing cookie name and value cannot be empty",
		},
		{
			name:        "ServerTimingCookie empty value",
			opt:         WithServerTimingCookie("debug", ""),
			errContains: "server timing cookie name and value cannot be empty",
		},
		{
			name:        "ServerTimingCookie whitespace value",
			opt:         WithServerTimingCookie("debug", "   "),
			errContains: "server timing cookie name and value cannot be empty",
		},
		{
			name:        "ESI option nil",
			opt:         WithESI(nil),
			errContains: "ESI option cannot be nil",
		},
		{
			name:        "Logger nil",
			opt:         WithLogger(nil),
			errContains: "logger cannot be nil",
		},
		{
			name:        "Metrics nil",
			opt:         WithMetrics(nil),
			errContains: "metrics registerer cannot be nil",
		},
		{
			name:        "Top-level option nil",
			opt:         nil,
			errContains: "option cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(store, tt.opt)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.errContains)
			}
			if !errors.Is(err, ErrInvalidOption) {
				t.Errorf("expected error to wrap ErrInvalidOption, got: %v", err)
			}
			if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("expected error message to contain %q, got: %v", tt.errContains, err)
			}
		})
	}
}

func TestNew_ESIOptionErrorChaining(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	_, err := New(store, WithESI(esi.WithMaxDepth(0)))
	if err == nil {
		t.Fatalf("expected error from invalid ESI option, got nil")
	}

	if !errors.Is(err, ErrInvalidOption) {
		t.Errorf("expected error to wrap titip.ErrInvalidOption, got: %v", err)
	}
	if !errors.Is(err, esi.ErrInvalidOption) {
		t.Errorf("expected error to wrap esi.ErrInvalidOption, got: %v", err)
	}
	if !strings.Contains(err.Error(), "max depth must be greater than 0") {
		t.Errorf("expected error to contain inner ESI error message, got: %v", err)
	}
}

func TestBackgroundFetchTimeout_Configuration(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	// 1. Custom timeout
	mwCustom, err := New(
		store,
		WithBackgroundFetchTimeout(60*time.Second),
	)
	if err != nil {
		t.Fatalf("failed to create mw: %v", err)
	}
	if mwCustom.config.backgroundFetchTimeout != 60*time.Second {
		t.Errorf("expected 60s, got %v", mwCustom.config.backgroundFetchTimeout)
	}

	// 2. Disabled timeout (0)
	mwDisabled, err := New(
		store,
		WithBackgroundFetchTimeout(0),
	)
	if err != nil {
		t.Fatalf("failed to create mw: %v", err)
	}
	if mwDisabled.config.backgroundFetchTimeout != 0 {
		t.Errorf("expected 0, got %v", mwDisabled.config.backgroundFetchTimeout)
	}
}

func TestConfig_LoggerAndMetrics(t *testing.T) {
	t.Parallel()
	store := teststore.New()

	logger := slog.New(slog.DiscardHandler)
	reg := prometheus.NewRegistry()

	mw, err := New(
		store,
		WithLogger(logger),
		WithMetrics(reg),
	)
	if err != nil {
		t.Fatalf("failed to create mw: %v", err)
	}
	if mw.config.logger != logger {
		t.Errorf("expected logger to be set")
	}
	if mw.config.metrics != reg {
		t.Errorf("expected metrics to be set")
	}
}
