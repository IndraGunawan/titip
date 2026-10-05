package titip

import (
	"sync"
	"testing"
)

func TestBufferPool(t *testing.T) {
	t.Parallel()
	buf := getBuffer()
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	buf.WriteString("hello world")
	if buf.String() != "hello world" {
		t.Fatalf("unexpected buffer content: %s", buf.String())
	}
	putBuffer(buf)

	// Get buffer again, should be reset (empty)
	buf2 := getBuffer()
	if buf2.Len() != 0 {
		t.Fatalf("expected reset buffer with len 0, got %d", buf2.Len())
	}
	putBuffer(buf2)
}

func TestBufferPoolGrowthProtection(t *testing.T) {
	t.Parallel()
	buf := getBuffer()
	// Grow buffer beyond 2MB
	largeData := make([]byte, 3*1024*1024)
	buf.Write(largeData)
	if buf.Cap() <= maxBufferSize {
		t.Fatalf("expected buffer cap > %d, got %d", maxBufferSize, buf.Cap())
	}

	// Putting large buffer should discard it without panic
	putBuffer(buf)

	// Getting a buffer should work cleanly
	buf2 := getBuffer()
	if buf2.Cap() > maxBufferSize {
		t.Fatalf("expected fresh pooled buffer, got cap %d", buf2.Cap())
	}
	putBuffer(buf2)
}

func TestBufferPoolConcurrencyAndRaces(t *testing.T) {
	t.Parallel()
	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(id int) {
			defer wg.Done()

			for range iterations {
				buf := getBuffer()
				buf.WriteString("concurrency test string")
				putBuffer(buf)
			}
		}(i)
	}

	wg.Wait()
}

func BenchmarkBufferPool(b *testing.B) {
	for b.Loop() {
		buf := getBuffer()
		buf.WriteString("benchmark buffer data payload")
		putBuffer(buf)
	}
}
