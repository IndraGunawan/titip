package esi

import (
	"bytes"
	"testing"

	proto "github.com/indragunawan/titip/proto"
)

func TestScanner_BasicSelfClosing(t *testing.T) {
	html := []byte(`<!DOCTYPE html><html><body><esi:include src="/api/user" /></body></html>`)
	frags := Scan(html)

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
	if frag.InnerStartPos != 0 || frag.InnerEndPos != 0 {
		t.Errorf("expected 0 inner positions, got start=%d end=%d", frag.InnerStartPos, frag.InnerEndPos)
	}
}

func TestScanner_PairedWithFallback(t *testing.T) {
	html := []byte(`<div><esi:include src="/cart" alt="/cart-cached" timeout="0.5" max-depth="2" onerror="continue"><span>Default Cart</span></esi:include></div>`)
	frags := Scan(html)

	if len(frags) != 1 {
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
	if f.StartPos != 5 || f.EndPos != 135 {
		t.Errorf("expected bounds [5:135], got [%d:%d]", f.StartPos, f.EndPos)
	}
	if f.InnerStartPos != 96 || f.InnerEndPos != 121 {
		t.Errorf("expected inner bounds [96:121], got [%d:%d]", f.InnerStartPos, f.InnerEndPos)
	}
	if string(html[f.InnerStartPos:f.InnerEndPos]) != "<span>Default Cart</span>" {
		t.Errorf("expected inner content '<span>Default Cart</span>', got %q", string(html[f.InnerStartPos:f.InnerEndPos]))
	}
}

func TestScanner_InnerContentPositions(t *testing.T) {
	t.Run("multiple paired tags in single document", func(t *testing.T) {
		html := []byte(`<header><esi:include src="/nav">Fallback Nav</esi:include></header><main><esi:include src="/content">Fallback Content</esi:include></main><footer><esi:include src="/footer" /></footer>`)
		frags := Scan(html)
		if len(frags) != 3 {
			t.Fatalf("expected 3 fragments, got %d", len(frags))
		}

		// First paired tag
		if string(html[frags[0].InnerStartPos:frags[0].InnerEndPos]) != "Fallback Nav" {
			t.Errorf("tag 0 inner content mismatch: %q", string(html[frags[0].InnerStartPos:frags[0].InnerEndPos]))
		}

		// Second paired tag
		if string(html[frags[1].InnerStartPos:frags[1].InnerEndPos]) != "Fallback Content" {
			t.Errorf("tag 1 inner content mismatch: %q", string(html[frags[1].InnerStartPos:frags[1].InnerEndPos]))
		}

		// Third self-closing tag
		if frags[2].InnerStartPos != 0 || frags[2].InnerEndPos != 0 {
			t.Errorf("expected self-closing tag to have zero inner positions, got start=%d end=%d", frags[2].InnerStartPos, frags[2].InnerEndPos)
		}
	})

	t.Run("empty paired tag", func(t *testing.T) {
		html := []byte(`<div><esi:include src="/empty"></esi:include></div>`)
		frags := Scan(html)
		if len(frags) != 1 {
			t.Fatalf("expected 1 fragment, got %d", len(frags))
		}
		f := frags[0]
		if f.InnerStartPos != f.InnerEndPos {
			t.Errorf("expected equal InnerStartPos and InnerEndPos for empty paired tag, got start=%d end=%d", f.InnerStartPos, f.InnerEndPos)
		}
		if len(html[f.InnerStartPos:f.InnerEndPos]) != 0 {
			t.Errorf("expected empty slice, got %q", string(html[f.InnerStartPos:f.InnerEndPos]))
		}
	})

	t.Run("paired tag nested inside inline comment wrapper", func(t *testing.T) {
		html := []byte(`<div class="wrap"><!--esi <esi:include src="/user"><strong>Default User</strong></esi:include> --></div>`)
		frags := Scan(html)
		if len(frags) == 0 {
			t.Fatal("expected ESI detected")
		}
		var includeFrag *proto.EsiFragment
		for _, f := range frags {
			if f.Src == "/user" {
				includeFrag = f
				break
			}
		}
		if includeFrag == nil {
			t.Fatal("nested include fragment not found")
		}
		if string(html[includeFrag.InnerStartPos:includeFrag.InnerEndPos]) != "<strong>Default User</strong>" {
			t.Errorf("nested inner content mismatch: got %q, want %q",
				string(html[includeFrag.InnerStartPos:includeFrag.InnerEndPos]), "<strong>Default User</strong>")
		}
	})

	t.Run("multiline complex inner HTML", func(t *testing.T) {
		inner := "\n  <div class=\"widget\" data-id=\"42\">\n    <p>Loading...</p>\n  </div>\n"
		html := []byte(`<esi:include src="/widget">` + inner + `</esi:include>`)
		frags := Scan(html)
		if len(frags) != 1 {
			t.Fatalf("expected 1 fragment, got %d", len(frags))
		}
		f := frags[0]
		got := string(html[f.InnerStartPos:f.InnerEndPos])
		if got != inner {
			t.Errorf("multiline inner content mismatch:\ngot:  %q\nwant: %q", got, inner)
		}
	})
}

func TestScanner_MaxDepth_EdgeCases(t *testing.T) {
	tests := []struct {
		tag       string
		wantDepth uint32
	}{
		{`<esi:include src="/a" max-depth="5" />`, 5},
		{`<esi:include src="/a" max-depth="0" />`, 0},
		{`<esi:include src="/a" max-depth="invalid" />`, 0},
		{`<esi:include src="/a" max-depth="-1" />`, 0},
		{`<esi:include src="/a" max-depth="" />`, 0},
		{`<esi:include src="/a" max-depth="99999999999999999999" />`, 0},
	}

	for _, tt := range tests {
		frags := Scan([]byte(tt.tag))
		if len(frags) != 1 {
			t.Fatalf("expected 1 fragment for %s, got %d", tt.tag, len(frags))
		}
		if frags[0].MaxDepth != tt.wantDepth {
			t.Errorf("Scan(%s).MaxDepth = %d, want %d", tt.tag, frags[0].MaxDepth, tt.wantDepth)
		}
	}
}

func TestScanner_QuoteAwareClosingBracket(t *testing.T) {
	html := []byte(`<esi:include src="/api/search?q=foo>bar&sort=asc" />`)
	frags := Scan(html)

	if len(frags) != 1 {
		t.Fatalf("expected 1 fragment, got %d", len(frags))
	}
	if frags[0].Src != "/api/search?q=foo>bar&sort=asc" {
		t.Errorf("failed quote-aware attribute parsing: got %s", frags[0].Src)
	}
}

func TestScanner_RemoveBlock(t *testing.T) {
	html := []byte(`<h1>Hello</h1><esi:remove><p>This should be removed</p></esi:remove><p>World</p>`)
	frags := Scan(html)

	if len(frags) != 1 {
		t.Fatalf("expected 1 fragment for remove, got %d", len(frags))
	}
	if frags[0].Src != "" || frags[0].InnerStartPos != 0 || frags[0].InnerEndPos != 0 {
		t.Errorf("remove tag should have empty src and zero inner positions")
	}
	if string(html[frags[0].StartPos:frags[0].EndPos]) != "<esi:remove><p>This should be removed</p></esi:remove>" {
		t.Errorf("unexpected range for remove block: %s", html[frags[0].StartPos:frags[0].EndPos])
	}
}

func TestScanner_CommentTag(t *testing.T) {
	html := []byte(`<h1>Title</h1><esi:comment text="Internal comment" /><esi:comment>Block comment</esi:comment>`)
	frags := Scan(html)

	if len(frags) != 2 {
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
	t.Run("with inner include", func(t *testing.T) {
		html := []byte(`<!--esi <esi:include src="/footer" /> -->`)
		frags := Scan(html)

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
	})

	t.Run("with plain text only (inner scan returns nil)", func(t *testing.T) {
		html := []byte(`<!--esi <p>Client-hidden content unescaped by ESI</p> -->`)
		frags := Scan(html)

		// Should produce exactly 2 strip fragments (<!--esi and -->) while Scan(innerBlock) returns nil:
		if len(frags) != 2 {
			t.Fatalf("expected 2 fragments for plain inline comment unescape, got %d", len(frags))
		}
		if string(html[frags[0].StartPos:frags[0].EndPos]) != "<!--esi" {
			t.Errorf("frag 0 should be <!--esi, got %s", html[frags[0].StartPos:frags[0].EndPos])
		}
		if string(html[frags[1].StartPos:frags[1].EndPos]) != "-->" {
			t.Errorf("frag 1 should be -->, got %s", html[frags[1].StartPos:frags[1].EndPos])
		}
	})
}

func TestScanner_NoESI(t *testing.T) {
	html := []byte(`<html><body><p>Normal text without any directives</p></body></html>`)
	frags := Scan(html)
	if len(frags) != 0 {
		t.Errorf("expected no ESI, got len(frags)=%d", len(frags))
	}
}

func TestScanner_NilOrEmpty(t *testing.T) {
	frags := Scan(nil)
	if frags != nil {
		t.Errorf("expected nil for nil input, got %v", frags)
	}

	frags = Scan([]byte{})
	if frags != nil {
		t.Errorf("expected nil for empty input, got %v", frags)
	}
}

func TestScanner_AttributeVariations(t *testing.T) {
	tests := []struct {
		name        string
		html        string
		wantSrc     string
		wantAlt     string
		wantOnError string
	}{
		{
			name:    "single quoted attributes",
			html:    `<esi:include src='/api/user' alt='/api/guest' onerror='continue' />`,
			wantSrc: "/api/user", wantAlt: "/api/guest", wantOnError: "continue",
		},
		{
			name:    "unquoted attributes",
			html:    `<esi:include src=/api/user alt=/api/guest onerror=continue />`,
			wantSrc: "/api/user", wantAlt: "/api/guest", wantOnError: "continue",
		},
		{
			name:    "spaces around equals sign",
			html:    `<esi:include src = "/api/user" alt = "/api/guest" onerror = "continue" />`,
			wantSrc: "/api/user", wantAlt: "/api/guest", wantOnError: "continue",
		},
		{
			name:    "attribute prefix collision datasrc vs src",
			html:    `<esi:include datasrc="/wrong" src="/correct" customalt="/wrong2" alt="/correct2" />`,
			wantSrc: "/correct", wantAlt: "/correct2", wantOnError: "",
		},
		{
			name:    "attribute name without value at end of header",
			html:    `<esi:include src="/api/user" alt />`,
			wantSrc: "/api/user", wantAlt: "", wantOnError: "",
		},
		{
			name:    "attribute with trailing equals but no value",
			html:    `<esi:include src="/api/user" alt= />`,
			wantSrc: "/api/user", wantAlt: "", wantOnError: "",
		},
		{
			name:    "mixed single and double quotes",
			html:    `<esi:include src="/api/user" alt='/api/guest' onerror="continue" />`,
			wantSrc: "/api/user", wantAlt: "/api/guest", wantOnError: "continue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frags := Scan([]byte(tt.html))
			if len(frags) != 1 {
				t.Fatalf("expected 1 fragment, got len(frags)=%d", len(frags))
			}
			f := frags[0]
			if f.Src != tt.wantSrc {
				t.Errorf("Src mismatch: got %q, want %q", f.Src, tt.wantSrc)
			}
			if f.Alt != tt.wantAlt {
				t.Errorf("Alt mismatch: got %q, want %q", f.Alt, tt.wantAlt)
			}
			if f.OnError != tt.wantOnError {
				t.Errorf("OnError mismatch: got %q, want %q", f.OnError, tt.wantOnError)
			}
		})
	}
}

func TestScanner_TimeoutFormats(t *testing.T) {
	tests := []struct {
		timeoutStr string
		wantMs     int64
	}{
		{"500ms", 500},
		{"2s", 2000},
		{"1.5s", 1500},
		{"0.5", 500},
		{"2", 2000},
		{"   100ms   ", 100},
		{"invalid", 0},
		{"", 0},
		{"-1s", 0},
	}

	for _, tt := range tests {
		t.Run(tt.timeoutStr, func(t *testing.T) {
			html := `<esi:include src="/api" timeout="` + tt.timeoutStr + `" />`
			frags := Scan([]byte(html))
			if len(frags) != 1 {
				t.Fatalf("expected 1 fragment, got %d", len(frags))
			}
			if frags[0].TimeoutMs != tt.wantMs {
				t.Errorf("TimeoutMs mismatch for %q: got %d, want %d", tt.timeoutStr, frags[0].TimeoutMs, tt.wantMs)
			}
		})
	}
}

func TestScanner_MalformedAndUnclosed(t *testing.T) {
	t.Run("unclosed paired tag falls back to self closing", func(t *testing.T) {
		html := []byte(`<esi:include src="/stream">Content without close tag`)
		frags := Scan(html)
		if len(frags) != 1 {
			t.Fatalf("expected 1 fragment, got %d", len(frags))
		}
		if frags[0].Src != "/stream" {
			t.Errorf("unexpected src: %s", frags[0].Src)
		}
		if frags[0].InnerStartPos != 0 || frags[0].InnerEndPos != 0 {
			t.Errorf("expected zero inner positions for unclosed paired tag")
		}
	})

	t.Run("unclosed tag bracket at eof", func(t *testing.T) {
		html := []byte(`<esi:include src="/incomplete`)
		frags := Scan(html)
		if len(frags) != 0 {
			t.Errorf("expected no fragments for tag without closing bracket, got %d", len(frags))
		}
	})

	t.Run("similar tag name prefix not matched", func(t *testing.T) {
		html := []byte(`<esi:include_custom src="/ignore" /><esi:commentary>text</esi:commentary>`)
		frags := Scan(html)
		if len(frags) != 0 {
			t.Errorf("expected non-standard tags to be ignored, got len(frags)=%d", len(frags))
		}
	})

	t.Run("unclosed remove block at eof", func(t *testing.T) {
		html := []byte(`<esi:remove><p>No closing tag at eof`)
		frags := Scan(html)
		if len(frags) != 0 {
			t.Errorf("expected unclosed remove block to be ignored, got %d", len(frags))
		}
	})

	t.Run("unclosed comment block at eof", func(t *testing.T) {
		html := []byte(`<esi:comment><p>No closing tag at eof`)
		frags := Scan(html)
		if len(frags) != 0 {
			t.Errorf("expected unclosed comment block to be ignored, got %d", len(frags))
		}
	})

	t.Run("inline comment without close", func(t *testing.T) {
		html := []byte(`<!--esi <esi:include src="/frag" />`)
		frags := Scan(html)
		if len(frags) < 1 {
			t.Fatalf("expected unclosed inline comment to still process opening, got %d", len(frags))
		}
	})

	t.Run("unclosed attribute quote at eof", func(t *testing.T) {
		html := []byte(`<esi:include src="unterminated`)
		frags := Scan(html)
		if len(frags) != 0 {
			t.Errorf("expected unclosed quote tag to be ignored, got %d", len(frags))
		}
	})
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
		frags := Scan(data)
		if len(frags) == 0 {
			b.Fatal("failed scan")
		}
	}
}

func BenchmarkESIScanner_NoESI(b *testing.B) {
	html := []byte(`<!DOCTYPE html><html><head><title>Static Page</title></head><body><div class='container'><p>Hello World without any ESI directives.</p></div></body></html>`)

	for b.Loop() {
		frags := Scan(html)
		if len(frags) > 0 {
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
		frags := Scan(parentData)

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
	preCompiledFrags := Scan(parentData)

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

func BenchmarkESIScanner_PairedWithInnerContent(b *testing.B) {
	html := []byte(`<div><esi:include src="/cart" alt="/cached" timeout="500ms"><div class="cart-fallback">Default Fallback Content That Used To Be Cloned</div></esi:include></div>`)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		frags := Scan(html)
		if len(frags) != 1 {
			b.Fatal("scan failed")
		}
	}
}
