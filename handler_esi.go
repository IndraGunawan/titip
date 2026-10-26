package titip

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/indragunawan/titip/esi"
	pb "github.com/indragunawan/titip/proto"
)

// inspectAndPrepareESI checks if the origin response satisfies ESI processing criteria.
// If so, it strips ESI directives from Surrogate-Control in-place and scans the body
// for ESI fragments, returning the parsed fragments.
func (t *Titip) inspectAndPrepareESI(h http.Header, body []byte) []*pb.EsiFragment {
	if t.esiProcessor == nil || !t.esiProcessor.CanProcess(h) {
		return nil
	}
	esi.CleanSurrogateControl(h)
	return esi.Scan(body)
}

// processESI delegates ESI fragment resolution, concurrent fetching,
// recursion, and document splicing to esi.Processor, reconciles response headers,
// and streams the result to the client writer.
func (t *Titip) processESI(
	ctx *requestContext,
	parentBody []byte,
	fragments []*pb.EsiFragment,
	statusCode int,
	headers http.Header,
	simpleStatus string,
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
			if t.logger.Enabled(ctx.r.Context(), slog.LevelWarn) {
				t.logger.WarnContext(ctx.r.Context(), "esi: processing failed, serving unspliced body", "error", err)
			}
			t.esiProcessor.ReconcileHeaders(reconciled, nil)
		} else {
			defer res.Release()
			body = res.Body()
			t.esiProcessor.ReconcileHeaders(reconciled, res)
			if ctx.timing.enabled {
				ctx.timing.esiDuration = res.Duration()
				ctx.timing.esiFragments = len(fragments)
			}
			if t.config.cacheStatusMode == CacheStatusRFC9211 {
				detail = rfc9211Detail + "; detail=\"esi-includes=" + strconv.Itoa(len(fragments)) + ";time=" + res.Duration().String() + "\""
			}
		}
	} else {
		esi.CleanSurrogateControl(reconciled)
	}

	for k, vv := range reconciled {
		if !isHopByHopHeader(k) {
			for _, v := range vv {
				ctx.w.Header().Add(k, v)
			}
		}
	}

	t.emitCacheStatus(ctx, simpleStatus, detail)
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
