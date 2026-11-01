package esi

import (
	"bytes"
	"net/http"
	"sync"
)

// subrequestRecorder captures in-process HTTP subrequest responses with pooled memory buffers.
type subrequestRecorder struct {
	code        int
	header      http.Header
	body        *bytes.Buffer
	wroteHeader bool
}

var subrequestRecorderPool = sync.Pool{
	New: func() any {
		return &subrequestRecorder{
			code:   http.StatusOK,
			header: make(http.Header),
			body:   new(bytes.Buffer),
		}
	},
}

func acquireSubrequestRecorder() *subrequestRecorder {
	return subrequestRecorderPool.Get().(*subrequestRecorder)
}

func releaseSubrequestRecorder(rec *subrequestRecorder) {
	if rec == nil {
		return
	}
	if rec.body == nil || rec.body.Cap() > maxBufferSize {
		rec.body = new(bytes.Buffer)
	}
	rec.code = http.StatusOK
	rec.wroteHeader = false
	if rec.header != nil {
		clear(rec.header)
	}
	if rec.body != nil {
		rec.body.Reset()
	}
	subrequestRecorderPool.Put(rec)
}

// Header returns the response header map.
func (r *subrequestRecorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

// detachHeader extracts and returns the recorder's header map, leaving
// the recorder's internal header reference nil so it can be lazily reallocated
// on subsequent use. This avoids allocating a new cloned map on the hot path.
func (r *subrequestRecorder) detachHeader() http.Header {
	h := r.header
	r.header = nil
	return h
}

// Write writes data to the internal buffer.
func (r *subrequestRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.code = http.StatusOK
		r.wroteHeader = true
	}
	if r.body == nil {
		r.body = new(bytes.Buffer)
	}
	return r.body.Write(b)
}

// WriteHeader records the HTTP status code.
func (r *subrequestRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.code = code
	r.wroteHeader = true
}
