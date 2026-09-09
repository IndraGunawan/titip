package titip

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	pb "github.com/indragunawan/titip/proto"
)

// processESI delegates ESI fragment resolution, concurrent fetching,
// recursion, and document splicing to esi.Processor, reconciles response headers,
// and streams the result to the client writer.
func (t *Titip) processESI(
	ctx *requestContext,
	parentBody []byte,
	fragments []*pb.EsiFragment,
	statusCode int,
	headers http.Header,
	statusToken string,
	rfc9211Detail string,
) {
	if t.esiProcessor == nil {
		t.writeFallback(ctx, parentBody, statusCode, headers, statusToken, rfc9211Detail)
		return
	}

	res, err := t.esiProcessor.Process(ctx.r.Context(), ctx.r, parentBody, fragments)
	if err != nil {
		if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
			t.logger.ErrorContext(ctx.r.Context(), "esi: processing failed, falling back to unspliced body", "error", err)
		}
		t.writeFallback(ctx, parentBody, statusCode, headers, statusToken, rfc9211Detail)
		return
	}
	defer res.Release()

	splicedBytes := res.Body()

	// Header reconciliation
	reconciledHeaders := headers.Clone()
	if reconciledHeaders == nil {
		reconciledHeaders = make(http.Header)
	}

	// Strip ESI/edge internal headers
	reconciledHeaders.Del(headerSurrogateControl)

	// ETag & Last-Modified handling for composite ESI documents:
	// By default (PreserveETag == false), strip ETag and Last-Modified to prevent downstream clients
	// from sending conditional requests that would skip live fragment assembly.
	// When PreserveETag == true (opt-in for static ESI), weaken ETag to W/"..." per RFC 9110 §8.8.3.2.
	if t.esiProcessor.PreserveETag() {
		if etag := reconciledHeaders.Get(headerETag); etag != "" {
			if !strings.HasPrefix(etag, "W/") && !strings.HasPrefix(etag, "w/") {
				reconciledHeaders.Set(headerETag, "W/"+etag)
			}
		}
	} else {
		reconciledHeaders.Del(headerETag)
		reconciledHeaders.Del(headerLastModified)
	}

	// Update Content-Length only if the origin explicitly provided one
	if reconciledHeaders.Get(headerContentLength) != "" {
		reconciledHeaders.Set(headerContentLength, strconv.Itoa(len(splicedBytes)))
	}

	// Copy reconciled headers to client writer
	for k, vv := range reconciledHeaders {
		if !isHopByHopHeader(k) {
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}
	}

	// Forward dynamic subrequest Set-Cookie headers to live client
	for _, cookie := range res.SetCookies {
		ctx.w.Header().Add("Set-Cookie", cookie)
	}

	// Emit Cache-Status
	detailWithESI := fmt.Sprintf("%s; detail=\"esi-includes=%d;time=%s\"", rfc9211Detail, len(fragments), res.Duration.String())
	t.emitCacheStatus(ctx.w, statusToken, detailWithESI)

	// Write status and response body
	ctx.w.WriteHeader(statusCode)
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(splicedBytes)
	}
}

// writeFallback writes the unspliced parent body and original headers when ESI is unavailable or fails.
func (t *Titip) writeFallback(
	ctx *requestContext,
	parentBody []byte,
	statusCode int,
	headers http.Header,
	statusToken string,
	rfc9211Detail string,
) {
	for k, vv := range headers {
		if !isHopByHopHeader(k) && !strings.EqualFold(k, headerSurrogateControl) {
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}
	}
	t.emitCacheStatus(ctx.w, statusToken, rfc9211Detail)
	ctx.w.WriteHeader(statusCode)
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(parentBody)
	}
}
