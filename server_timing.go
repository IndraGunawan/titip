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

// serverTimingRecorder captures stage measurements and diagnostics for Server-Timing headers.
// Embedded by value into requestContext with zero heap allocations.
type serverTimingRecorder struct {
	enabled        bool
	startNano      int64
	metaDuration   time.Duration
	bodyDuration   time.Duration
	originDuration time.Duration
	storeDuration  time.Duration
	storeCodec     string
	storeRawSize   int64
	storeCompSize  int64
	esiDuration    time.Duration
	esiFragments   int
}

func (cfg serverTimingConfig) emit(w http.ResponseWriter, rec *serverTimingRecorder, statusToken string) {
	if rec == nil || !rec.enabled {
		return
	}
	buf := getBuffer()
	defer putBuffer(buf)

	var scratch [24]byte

	// 1. Status metric: titip-status;desc="HIT"
	if statusToken != "" {
		buf.WriteString("titip-status;desc=\"")
		buf.WriteString(statusToken)
		buf.WriteByte('"')
	}

	appendTimingMetric(buf, &scratch, "titip-meta", rec.metaDuration)
	if rec.bodyDuration > 0 {
		appendTimingMetric(buf, &scratch, "titip-body", rec.bodyDuration)
		if rec.storeCodec != "" && rec.storeCodec != StorageCompressionNone {
			buf.WriteString(";desc=\"")
			buf.WriteString(rec.storeCodec)
			buf.WriteByte('"')
		}
	}
	appendTimingMetric(buf, &scratch, "titip-origin", rec.originDuration)
	if rec.storeDuration > 0 {
		appendTimingMetric(buf, &scratch, "titip-store", rec.storeDuration)
		if rec.storeCodec != "" {
			buf.WriteString(";desc=\"")
			buf.WriteString(rec.storeCodec)
			if rec.storeRawSize > 0 {
				buf.WriteString(" (")
				appendByteSize(buf, &scratch, rec.storeRawSize)
				if rec.storeCompSize > 0 && rec.storeCodec != StorageCompressionNone {
					buf.WriteString(" -> ")
					appendByteSize(buf, &scratch, rec.storeCompSize)
					savedPct := 100 - (rec.storeCompSize*100)/rec.storeRawSize
					buf.WriteString(", -")
					buf.Write(strconv.AppendInt(scratch[:0], savedPct, 10))
					buf.WriteByte('%')
				}
				buf.WriteByte(')')
			}
			buf.WriteByte('"')
		}
	}

	if rec.esiDuration > 0 {
		appendTimingMetric(buf, &scratch, "titip-esi", rec.esiDuration)
		if rec.esiFragments > 0 {
			buf.WriteString(";desc=\"")
			buf.Write(strconv.AppendInt(scratch[:0], int64(rec.esiFragments), 10))
			buf.WriteString(" fragments\"")
		}
	}

	if rec.startNano > 0 {
		totalDur := time.Duration(time.Now().UnixNano() - rec.startNano)
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

func appendByteSize(buf *bytes.Buffer, scratch *[24]byte, size int64) {
	if size < 1024 {
		buf.Write(strconv.AppendInt(scratch[:0], size, 10))
		buf.WriteByte('B')
		return
	}
	if size < 1024*1024 {
		kb := float64(size) / 1024.0
		buf.Write(strconv.AppendFloat(scratch[:0], kb, 'f', 1, 64))
		buf.WriteString("KB")
		return
	}
	mb := float64(size) / (1024.0 * 1024.0)
	buf.Write(strconv.AppendFloat(scratch[:0], mb, 'f', 2, 64))
	buf.WriteString("MB")
}
