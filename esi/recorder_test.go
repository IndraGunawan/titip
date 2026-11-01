package esi

import (
	"net/http"
	"sync"
	"testing"
)

func TestSubrequestRecorder(t *testing.T) {
	t.Parallel()

	rec := acquireSubrequestRecorder()
	defer releaseSubrequestRecorder(rec)

	rec.Header().Set("Content-Type", "text/html")
	rec.WriteHeader(http.StatusCreated)

	payload := []byte("<span>Content</span>")
	n, err := rec.Write(payload)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected %d bytes written, got %d", len(payload), n)
	}

	if rec.code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rec.code)
	}
	if rec.Header().Get("Content-Type") != "text/html" {
		t.Fatalf("expected text/html header, got %s", rec.Header().Get("Content-Type"))
	}
	if rec.body.String() != string(payload) {
		t.Fatalf("expected body %s, got %s", payload, rec.body.String())
	}
}

func TestSubrequestRecorderImplicitStatus200(t *testing.T) {
	t.Parallel()

	rec := acquireSubrequestRecorder()
	defer releaseSubrequestRecorder(rec)

	_, err := rec.Write([]byte("implicit 200"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if rec.code != http.StatusOK {
		t.Fatalf("expected implicit status 200, got %d", rec.code)
	}
}

func TestSubrequestRecorderPoolRecyclingAndOversized(t *testing.T) {
	t.Parallel()

	// 1. Normal recycling resets state
	rec := acquireSubrequestRecorder()
	rec.Header().Set("X-Test", "123")
	rec.WriteHeader(http.StatusAccepted)
	_, _ = rec.Write([]byte("test data"))
	releaseSubrequestRecorder(rec)

	rec2 := acquireSubrequestRecorder()
	defer releaseSubrequestRecorder(rec2)
	if rec2.code != http.StatusOK {
		t.Errorf("expected reset code 200, got %d", rec2.code)
	}
	if len(rec2.header) != 0 {
		t.Errorf("expected clean header map, got %v", rec2.header)
	}
	if rec2.body.Len() != 0 {
		t.Errorf("expected empty body, got %d bytes", rec2.body.Len())
	}

	// 2. Oversized buffer growth protection
	recOversized := acquireSubrequestRecorder()
	oversizedBytes := make([]byte, maxBufferSize+1)
	_, _ = recOversized.Write(oversizedBytes)
	releaseSubrequestRecorder(recOversized)

	recRecovered := acquireSubrequestRecorder()
	defer releaseSubrequestRecorder(recRecovered)
	if recRecovered.body.Cap() > maxBufferSize {
		t.Errorf("expected capped buffer capacity <= %d, got %d", maxBufferSize, recRecovered.body.Cap())
	}

	// 3. Nil release safety
	releaseSubrequestRecorder(nil)
}

func TestSubrequestRecorderConcurrencyAndRaces(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			rec := acquireSubrequestRecorder()
			defer releaseSubrequestRecorder(rec)

			rec.Header().Set("Content-Type", "text/plain")
			rec.WriteHeader(http.StatusOK)
			_, _ = rec.Write([]byte("concurrent"))

			if rec.code != http.StatusOK {
				t.Errorf("expected 200, got %d", rec.code)
			}
		})
	}
	wg.Wait()
}
