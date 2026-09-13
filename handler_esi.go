package titip

import (
	"log/slog"
	"net/http"
	"strconv"

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
	var (
		body   = parentBody
		detail = rfc9211Detail
	)

	reconciled := headers.Clone()
	if reconciled == nil {
		reconciled = make(http.Header)
	}

	if t.esiProcessor != nil {
		res, err := t.esiProcessor.ProcessFragments(ctx.r.Context(), ctx.r, parentBody, fragments)
		if err != nil {
			if t.logger.Enabled(ctx.r.Context(), slog.LevelError) {
				t.logger.ErrorContext(ctx.r.Context(), "esi: processing failed, serving unspliced body", "error", err)
			}
			t.esiProcessor.ReconcileHeaders(reconciled, nil)
		} else {
			defer res.Release()
			body = res.Body()
			t.esiProcessor.ReconcileHeaders(reconciled, res)
			if ctx.serverTiming {
				ctx.esiDuration = res.Duration()
				ctx.esiFragments = len(fragments)
			}
			if t.config.cacheStatusMode == CacheStatusRFC9211 {
				detail = rfc9211Detail + "; detail=\"esi-includes=" + strconv.Itoa(len(fragments)) + ";time=" + res.Duration().String() + "\""
			}
		}
	} else {
		reconciled.Del(headerSurrogateControl)
	}

	for k, vv := range reconciled {
		if !isHopByHopHeader(k) {
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}
	}

	t.emitCacheStatus(ctx, statusToken, detail)
	ctx.w.WriteHeader(statusCode)
	if ctx.r.Method != http.MethodHead {
		_, _ = ctx.w.Write(body)
	}
}

// adjustESIHeaders adjusts downstream headers for 304 Not Modified and HEAD cached responses containing ESI fragments.
func (t *Titip) adjustESIHeaders(w http.ResponseWriter, varInfo *pb.VariantInfo) {
	if t.esiProcessor == nil || varInfo == nil || len(varInfo.EsiFragments) == 0 {
		return
	}
	t.esiProcessor.ReconcileHeaders(w.Header(), nil)
}
