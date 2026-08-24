package esi

import (
	"bytes"
	"testing"
)

func TestScanner_BasicSelfClosing(t *testing.T) {
	html := []byte(`<!DOCTYPE html><html><body><esi:include src="/api/user" /></body></html>`)
	hasESI, frags := Scan(html)

	if !hasESI {
		t.Fatalf("expected hasESI to be true")
	}
	if len(frags) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(frags))
	}

	frag := frags[0]
	if frag.Src != "/api/user" {
		t.Errorf("expected src /api/user, got %s", frag.Src)
	}
	if frag.StartPos != 27 {
		t.Errorf("expected StartPos 27, got %d", frag.StartPos)
	}
	if frag.EndPos != 58 {
		t.Errorf("expected EndPos 58, got %d", frag.EndPos)
	}
	if frag.FallbackBody != nil {
		t.Errorf("expected nil fallback body, got %s", frag.FallbackBody)
	}
}

func TestScanner_PairedWithFallback(t *testing.T) {
	html := []byte(`<div><esi:include src="/cart" alt="/cart-cached" timeout="0.5" max-depth="2" onerror="continue"><span>Default Cart</span></esi:include></div>`)
	hasESI, frags := Scan(html)

	if !hasESI || len(frags) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(frags))
	}

	f := frags[0]
	if f.Src != "/cart" {
		t.Errorf("src mismatch: %s", f.Src)
	}
	if f.Alt != "/cart-cached" {
		t.Errorf("alt mismatch: %s", f.Alt)
	}
	if f.TimeoutMs != 500 {
		t.Errorf("expected TimeoutMs 500, got %d", f.TimeoutMs)
	}
	if f.MaxDepth != 2 {
		t.Errorf("expected MaxDepth 2, got %d", f.MaxDepth)
	}
	if f.OnError != "continue" {
		t.Errorf("expected OnError continue, got %s", f.OnError)
	}
	if string(f.FallbackBody) != "<span>Default Cart</span>" {
		t.Errorf("expected fallback body '<span>Default Cart</span>', got %q", string(f.FallbackBody))
	}
}

func TestScanner_QuoteAwareClosingBracket(t *testing.T) {
	html := []byte(`<esi:include src="/api/search?q=foo>bar&sort=asc" />`)
	hasESI, frags := Scan(html)

	if !hasESI || len(frags) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(frags))
	}
	if frags[0].Src != "/api/search?q=foo>bar&sort=asc" {
		t.Errorf("failed quote-aware attribute parsing: got %s", frags[0].Src)
	}
}

func TestScanner_RemoveBlock(t *testing.T) {
	html := []byte(`<h1>Hello</h1><esi:remove><p>This should be removed</p></esi:remove><p>World</p>`)
	hasESI, frags := Scan(html)

	if !hasESI || len(frags) != 1 {
		t.Fatalf("expected 1 fragment for remove, got %d", len(frags))
	}
	if frags[0].Src != "" || frags[0].FallbackBody != nil {
		t.Errorf("remove tag should have empty src and nil fallback body")
	}
	if string(html[frags[0].StartPos:frags[0].EndPos]) != "<esi:remove><p>This should be removed</p></esi:remove>" {
		t.Errorf("unexpected range for remove block: %s", html[frags[0].StartPos:frags[0].EndPos])
	}
}

func TestScanner_CommentTag(t *testing.T) {
	html := []byte(`<h1>Title</h1><esi:comment text="Internal comment" /><esi:comment>Block comment</esi:comment>`)
	hasESI, frags := Scan(html)

	if !hasESI || len(frags) != 2 {
		t.Fatalf("expected 2 fragments for comments, got %d", len(frags))
	}
	if string(html[frags[0].StartPos:frags[0].EndPos]) != `<esi:comment text="Internal comment" />` {
		t.Errorf("unexpected comment 1: %s", html[frags[0].StartPos:frags[0].EndPos])
	}
	if string(html[frags[1].StartPos:frags[1].EndPos]) != `<esi:comment>Block comment</esi:comment>` {
		t.Errorf("unexpected comment 2: %s", html[frags[1].StartPos:frags[1].EndPos])
	}
}

func TestScanner_InlineCommentUnescape(t *testing.T) {
	html := []byte(`<!--esi <esi:include src="/footer" /> -->`)
	hasESI, frags := Scan(html)

	if !hasESI {
		t.Fatalf("expected hasESI to be true")
	}
	// Should produce 3 fragments:
	// 1: <!--esi prefix (stripped)
	// 2: <esi:include src="/footer" /> (executed)
	// 3: --> suffix (stripped)
	if len(frags) != 3 {
		t.Fatalf("expected 3 fragments for inline comment unescape, got %d", len(frags))
	}

	if string(html[frags[0].StartPos:frags[0].EndPos]) != "<!--esi" {
		t.Errorf("frag 0 should be <!--esi, got %s", html[frags[0].StartPos:frags[0].EndPos])
	}
	if frags[1].Src != "/footer" {
		t.Errorf("frag 1 should be /footer include, got %s", frags[1].Src)
	}
	if string(html[frags[2].StartPos:frags[2].EndPos]) != "-->" {
		t.Errorf("frag 2 should be -->, got %s", html[frags[2].StartPos:frags[2].EndPos])
	}
}

func TestScanner_NoESI(t *testing.T) {
	html := []byte(`<html><body><p>Normal text without any directives</p></body></html>`)
	hasESI, frags := Scan(html)
	if hasESI || frags != nil {
		t.Errorf("expected no ESI, got hasESI=%v, len(frags)=%d", hasESI, len(frags))
	}
}

func BenchmarkESIScanner_MultiTag(b *testing.B) {
	// 50KB HTML document with 5 includes and comments
	var buf bytes.Buffer
	buf.WriteString("<!DOCTYPE html><html><head><title>Test Benchmark</title></head><body>\n")
	for i := range 500 {
		buf.WriteString("<div class='section'><p>Paragraph content lorem ipsum dolor sit amet...</p></div>\n")
		switch i {
		case 50:
			buf.WriteString("<esi:include src=\"/api/header\" timeout=\"1s\" />\n")
		case 150:
			buf.WriteString("<esi:include src=\"/api/cart\" max-depth=\"2\"><p>Cart Loading...</p></esi:include>\n")
		case 250:
			buf.WriteString("<esi:remove><p>Client JS fallback only</p></esi:remove>\n")
		case 350:
			buf.WriteString("<esi:comment text=\"analytics placeholder\" />\n")
		case 450:
			buf.WriteString("<!--esi <esi:include src=\"/api/footer\" /> -->\n")
		}
	}
	buf.WriteString("</body></html>")
	data := buf.Bytes()

	for b.Loop() {
		hasESI, frags := Scan(data)
		if !hasESI || len(frags) == 0 {
			b.Fatal("failed scan")
		}
	}
}

func BenchmarkESIScanner_NoESI(b *testing.B) {
	html := []byte(`<!DOCTYPE html><html><head><title>Static Page</title></head><body><div class='container'><p>Hello World without any ESI directives.</p></div></body></html>`)

	for b.Loop() {
		hasESI, _ := Scan(html)
		if hasESI {
			b.Fatal("expected no ESI")
		}
	}
}

// BenchmarkESI_ScanAndSplice_ColdMiss simulates complete Cold Miss processing:
// scanning 50KB HTML template + compiling descriptors + assembling output into a pooled buffer.
func BenchmarkESI_ScanAndSplice_ColdMiss(b *testing.B) {
	var buf bytes.Buffer
	buf.WriteString("<!DOCTYPE html><html><head><title>Test Benchmark</title></head><body>\n")
	for i := range 500 {
		buf.WriteString("<div class='section'><p>Paragraph content lorem ipsum dolor sit amet...</p></div>\n")
		switch i {
		case 50:
			buf.WriteString("<esi:include src=\"/api/header\" />\n")
		case 250:
			buf.WriteString("<esi:include src=\"/api/user\" />\n")
		case 450:
			buf.WriteString("<esi:include src=\"/api/footer\" />\n")
		}
	}
	buf.WriteString("</body></html>")
	parentData := buf.Bytes()

	fragPayloads := [][]byte{
		[]byte("<nav>Header Nav Menu</nav>"),
		[]byte("<span>User: Bob</span>"),
		[]byte("<footer>Site Footer 2026</footer>"),
	}

	var pooledBuf bytes.Buffer
	pooledBuf.Grow(len(parentData) + 1024)

	for b.Loop() {
		// 1. Scan from scratch on cold miss
		_, frags := Scan(parentData)

		// 2. Splice into recycled buffer
		pooledBuf.Reset()
		lastPos := 0
		for idx, frag := range frags {
			start := int(frag.StartPos)
			end := int(frag.EndPos)
			pooledBuf.Write(parentData[lastPos:start])
			pooledBuf.Write(fragPayloads[idx])
			lastPos = end
		}
		if lastPos < len(parentData) {
			pooledBuf.Write(parentData[lastPos:])
		}
	}
}

// BenchmarkESI_PreCompiled_CacheHit_PooledBuffer simulates complete Cache Hit processing:
// zero scanner called, using pre-compiled metadata from Redis and splicing into a pooled buffer.
func BenchmarkESI_PreCompiled_CacheHit_PooledBuffer(b *testing.B) {
	var buf bytes.Buffer
	buf.WriteString("<!DOCTYPE html><html><head><title>Test Benchmark</title></head><body>\n")
	for i := range 500 {
		buf.WriteString("<div class='section'><p>Paragraph content lorem ipsum dolor sit amet...</p></div>\n")
		switch i {
		case 50:
			buf.WriteString("<esi:include src=\"/api/header\" />\n")
		case 250:
			buf.WriteString("<esi:include src=\"/api/user\" />\n")
		case 450:
			buf.WriteString("<esi:include src=\"/api/footer\" />\n")
		}
	}
	buf.WriteString("</body></html>")
	parentData := buf.Bytes()

	// Pre-compile once (as stored in Redis on initial cold miss)
	_, preCompiledFrags := Scan(parentData)

	fragPayloads := [][]byte{
		[]byte("<nav>Header Nav Menu</nav>"),
		[]byte("<span>User: Bob</span>"),
		[]byte("<footer>Site Footer 2026</footer>"),
	}

	var pooledBuf bytes.Buffer
	pooledBuf.Grow(len(parentData) + 1024)

	for b.Loop() {
		// Zero scanner called; direct buffer splicing into pooled buffer
		pooledBuf.Reset()
		lastPos := 0
		for idx, frag := range preCompiledFrags {
			start := int(frag.StartPos)
			end := int(frag.EndPos)
			pooledBuf.Write(parentData[lastPos:start])
			pooledBuf.Write(fragPayloads[idx])
			lastPos = end
		}
		if lastPos < len(parentData) {
			pooledBuf.Write(parentData[lastPos:])
		}
	}
}
