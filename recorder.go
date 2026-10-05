package titip

import (
	"bytes"
	"net/http"
	"sync"
)

// responseRecorder intercepts and records HTTP responses from upstream origin handlers.
type responseRecorder struct {
	Code        int
	HeaderMap   http.Header
	Body        *bytes.Buffer
	wroteHeader bool
	flushed     bool
}

var responseRecorderPool = sync.Pool{
	New: func() any {
		return &responseRecorder{
			Code:      http.StatusOK,
			HeaderMap: make(http.Header),
			Body:      new(bytes.Buffer),
		}
	},
}

// getResponseRecorder retrieves a pooled responseRecorder.
func getResponseRecorder() *responseRecorder {
	return responseRecorderPool.Get().(*responseRecorder)
}

// putResponseRecorder cleans and returns a responseRecorder to the pool.
func putResponseRecorder(rec *responseRecorder) {
	if rec == nil {
		return
	}
	if rec.Body == nil || rec.Body.Cap() > maxBufferSize {
		rec.Body = new(bytes.Buffer)
	}
	rec.Reset()
	responseRecorderPool.Put(rec)
}

// Header returns the response headers map.
func (rec *responseRecorder) Header() http.Header {
	if rec.HeaderMap == nil {
		rec.HeaderMap = make(http.Header)
	}
	return rec.HeaderMap
}

// Write writes data to the internal response buffer.
func (rec *responseRecorder) Write(b []byte) (int, error) {
	if !rec.wroteHeader {
		rec.WriteHeader(http.StatusOK)
	}
	if rec.Body == nil {
		rec.Body = new(bytes.Buffer)
	}
	return rec.Body.Write(b)
}

// WriteHeader records the HTTP status code.
func (rec *responseRecorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.Code = code
	rec.wroteHeader = true
}

// Flush implements http.Flusher.
func (rec *responseRecorder) Flush() {
	rec.flushed = true
}

// WroteHeader returns true if WriteHeader has been called.
func (rec *responseRecorder) WroteHeader() bool {
	return rec.wroteHeader
}

// Reset resets the recorder state for reuse.
func (rec *responseRecorder) Reset() {
	rec.Code = http.StatusOK
	rec.wroteHeader = false
	rec.flushed = false
	if rec.HeaderMap != nil {
		clear(rec.HeaderMap)
	}
	if rec.Body != nil {
		rec.Body.Reset()
	}
}
