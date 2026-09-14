package titip

import (
	"bytes"
	"net/http"
	"strconv"
	"time"
)

type serverTimingConfig struct {
	active      bool
	cookieName  string
	cookieValue string
}

func (cfg serverTimingConfig) enabled(r *http.Request) bool {
	if !cfg.active {
		return false
	}
	if cfg.cookieName == "" {
		return true
	}
	c, err := r.Cookie(cfg.cookieName)
	return err == nil && c.Value == cfg.cookieValue
}

func (cfg serverTimingConfig) emit(w http.ResponseWriter, ctx *requestContext, statusToken string) {
	buf := getBuffer()
	defer putBuffer(buf)

	var scratch [24]byte

	// 1. Status metric: titip-status;desc="HIT"
	if statusToken != "" {
		buf.WriteString("titip-status;desc=\"")
		buf.WriteString(statusToken)
		buf.WriteByte('"')
	}

	appendTimingMetric(buf, &scratch, "titip-meta", ctx.metaDuration)
	appendTimingMetric(buf, &scratch, "titip-body", ctx.bodyDuration)
	appendTimingMetric(buf, &scratch, "titip-origin", ctx.originDuration)
	appendTimingMetric(buf, &scratch, "titip-store", ctx.storeDuration)

	if ctx.esiDuration > 0 {
		appendTimingMetric(buf, &scratch, "titip-esi", ctx.esiDuration)
		if ctx.esiFragments > 0 {
			buf.WriteString(";desc=\"")
			buf.Write(strconv.AppendInt(scratch[:0], int64(ctx.esiFragments), 10))
			buf.WriteString(" fragments\"")
		}
	}

	if ctx.startNano > 0 {
		totalDur := time.Duration(time.Now().UnixNano() - ctx.startNano)
		appendTimingMetric(buf, &scratch, "titip", totalDur)
	}

	if buf.Len() > 0 {
		w.Header().Add("Server-Timing", buf.String())
	}
}

func appendTimingMetric(buf *bytes.Buffer, scratch *[24]byte, name string, d time.Duration) {
	if d <= 0 {
		return
	}
	if buf.Len() > 0 {
		buf.WriteString(", ")
	}
	buf.WriteString(name)
	buf.WriteString(";dur=")
	buf.Write(strconv.AppendFloat(scratch[:0], float64(d)/1e6, 'f', 2, 64))
}
