package titip

import (
	"net/http"
	"sync"
	"testing"
)

func TestResponseRecorder(t *testing.T) {
	t.Parallel()
	rec := getResponseRecorder()
	defer putResponseRecorder(rec)

	rec.Header().Set("Content-Type", "application/json")
	rec.Header().Add("X-Custom", "value1")
	rec.Header().Add("X-Custom", "value2")
	rec.WriteHeader(http.StatusCreated)

	payload := []byte(`{"status":"ok"}`)
	n, err := rec.Write(payload)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("expected %d bytes written, got %d", len(payload), n)
	}

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected application/json header, got %s", rec.Header().Get("Content-Type"))
	}
	if rec.Body.String() != string(payload) {
		t.Fatalf("expected body %s, got %s", payload, rec.Body.String())
	}
	if !rec.WroteHeader() {
		t.Fatal("expected WroteHeader() to be true")
	}

	rec.Flush()
	if !rec.flushed {
		t.Fatal("expected flushed to be true")
	}
}

func TestResponseRecorderImplicitStatus200(t *testing.T) {
	t.Parallel()
	rec := getResponseRecorder()
	defer putResponseRecorder(rec)

	_, err := rec.Write([]byte("default 200 ok"))
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected implicit status 200, got %d", rec.Code)
	}
	if !rec.WroteHeader() {
		t.Fatal("expected wroteHeader true")
	}
	rec.Flush()
	if !rec.flushed {
		t.Fatal("expected flushed to be true")
	}

	// Test ResponseRecorder resetting in pool
	rec = getResponseRecorder()
	rec.Header().Set("X-Test", "123")
	rec.WriteHeader(http.StatusAccepted)
	_, _ = rec.Write([]byte("temp"))
	putResponseRecorder(rec)

	rec2 := getResponseRecorder()
	defer putResponseRecorder(rec2)

	if len(rec2.Header()) != 0 {
		t.Fatalf("expected empty headers, got %v", rec2.Header())
	}
	if rec2.Body.Len() != 0 {
		t.Fatalf("expected empty body, got len %d", rec2.Body.Len())
	}
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected reset code 200, got %d", rec2.Code)
	}
	if rec2.WroteHeader() {
		t.Fatal("expected wroteHeader false")
	}
}

func TestResponseRecorderConcurrencyAndRaces(t *testing.T) {
	t.Parallel()
	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()

			for range iterations {
				rec := getResponseRecorder()
				rec.Header().Set("X-Goroutine", "test")
				rec.WriteHeader(http.StatusOK)
				_, _ = rec.Write([]byte("response body test"))
				putResponseRecorder(rec)
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkResponseRecorderPool(b *testing.B) {
	payload := []byte(`{"status":"cached"}`)

	for b.Loop() {
		rec := getResponseRecorder()
		rec.WriteHeader(http.StatusOK)
		_, _ = rec.Write(payload)
		putResponseRecorder(rec)
	}
}
