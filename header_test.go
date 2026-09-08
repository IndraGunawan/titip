package titip

import (
	"net/http"
	"slices"
	"testing"
)

func TestContainsToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		headerVal string
		target    string
		want      bool
	}{
		{"exact match", "gzip", "gzip", true},
		{"in comma list", "gzip, deflate, br", "deflate", true},
		{"case insensitive target", "gzip, Deflate, br", "deflate", true},
		{"case insensitive header", "gzip, DEFLATE, br", "Deflate", true},
		{"first element", "gzip, deflate", "gzip", true},
		{"last element", "gzip, deflate", "deflate", true},
		{"with spaces", "  gzip  ,   deflate  ", "deflate", true},
		{"not present", "gzip, deflate", "br", false},
		{"empty header", "", "gzip", false},
		{"empty target", "gzip, deflate", "", false},
		{"substring not token", "gzip, superdeflate", "deflate", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsToken(tt.headerVal, tt.target); got != tt.want {
				t.Errorf("containsToken(%q, %q) = %v, want %v", tt.headerVal, tt.target, got, tt.want)
			}
		})
	}
}

func TestGetHeaderValues(t *testing.T) {
	t.Parallel()

	t.Run("canonical key found", func(t *testing.T) {
		h := http.Header{}
		h.Add("Vary", "Accept")
		h.Add("Vary", "User-Agent")
		got := getHeaderValues(h, "Vary")
		want := []string{"Accept", "User-Agent"}
		if !slices.Equal(got, want) {
			t.Errorf("getHeaderValues() = %v, want %v", got, want)
		}
	})

	t.Run("case insensitive fallback", func(t *testing.T) {
		// Non-canonical key set directly on map
		h := http.Header{
			"cdn-cache-control": []string{"max-age=100"},
		}
		got := getHeaderValues(h, "CDN-Cache-Control")
		want := []string{"max-age=100"}
		if !slices.Equal(got, want) {
			t.Errorf("getHeaderValues() = %v, want %v", got, want)
		}
	})

	t.Run("header not found", func(t *testing.T) {
		h := http.Header{}
		got := getHeaderValues(h, "Non-Existent")
		if got != nil {
			t.Errorf("expected nil for non-existent header, got %v", got)
		}
	})
}

func TestSplitAndTrimTags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected []string
	}{
		{"tag1,,tag2", []string{"tag1", "tag2"}},
		{"tag1, ,tag2  tag3", []string{"tag1", "tag2", "tag3"}},
		{"tag1", []string{"tag1"}},
		{"tag1,tag2,tag3", []string{"tag1", "tag2", "tag3"}},
		{"tag1, tag2, tag3", []string{"tag1", "tag2", "tag3"}},
		{"  tag1  ,  tag2  ", []string{"tag1", "tag2"}},
		{",,tag1,,tag2,,", []string{"tag1", "tag2"}},
		{"tag1\t\ttag2", []string{"tag1", "tag2"}},
		{"tag1\ntag2\rtag3", []string{"tag1", "tag2", "tag3"}},
		{", , ,", nil},
		{"", nil},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := splitAndTrimTags(tt.input)
			if !slices.Equal(got, tt.expected) {
				t.Errorf("splitAndTrimTags(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestExtractTags(t *testing.T) {
	t.Parallel()

	t.Run("default tag header", func(t *testing.T) {
		h := http.Header{}
		h.Set(headerCacheTag, "alpha, beta,,gamma")
		tags := extractTags(h, "")
		expected := []string{"alpha", "beta", "gamma"}
		if !slices.Equal(tags, expected) {
			t.Errorf("extractTags() = %v, want %v", tags, expected)
		}

		// Empty header returns nil
		hEmpty := http.Header{}
		if got := extractTags(hEmpty, ""); got != nil {
			t.Errorf("expected nil for empty header, got %v", got)
		}
	})

	t.Run("custom tag header name", func(t *testing.T) {
		h := http.Header{}
		h.Set("X-Custom-Tags", "item1, item2")
		tags := extractTags(h, "X-Custom-Tags")
		expected := []string{"item1", "item2"}
		if !slices.Equal(tags, expected) {
			t.Errorf("extractTags() = %v, want %v", tags, expected)
		}
	})
}

func TestExtractVaryHeaderNames(t *testing.T) {
	t.Parallel()

	t.Run("single vary header", func(t *testing.T) {
		h := http.Header{}
		h.Set(headerVary, "Accept-Encoding, User-Agent")
		got := extractVaryHeaderNames(h)
		want := []string{"Accept-Encoding", "User-Agent"}
		if !slices.Equal(got, want) {
			t.Errorf("extractVaryHeaderNames() = %v, want %v", got, want)
		}
	})

	t.Run("multiple vary headers with deduplication", func(t *testing.T) {
		h := http.Header{}
		h.Add(headerVary, "Accept-Encoding, User-Agent")
		h.Add(headerVary, "User-Agent, Cookie, Accept-Encoding")
		got := extractVaryHeaderNames(h)
		want := []string{"Accept-Encoding", "User-Agent", "Cookie"}
		if !slices.Equal(got, want) {
			t.Errorf("extractVaryHeaderNames() = %v, want %v", got, want)
		}
	})

	t.Run("empty vary", func(t *testing.T) {
		h := http.Header{}
		got := extractVaryHeaderNames(h)
		if len(got) != 0 {
			t.Errorf("expected empty vary names, got %v", got)
		}
	})
}

func TestETagMatches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		client   string
		cached   string
		expected bool
	}{
		{`"123"`, `"123"`, true},
		{`W/"123"`, `"123"`, true},
		{`w/"123"`, `"123"`, true},
		{`"123"`, `W/"123"`, true},
		{`W/"123"`, `W/"123"`, true},
		{`"123"`, `"456"`, false},
		{`W/"123"`, `"456"`, false},
		{"", `"123"`, false},
		{`"123"`, "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		got := etagMatches(tt.client, tt.cached)
		if got != tt.expected {
			t.Errorf("etagMatches(%q, %q) = %v, want %v", tt.client, tt.cached, got, tt.expected)
		}
	}
}

func TestStrongETagMatches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		client   string
		cached   string
		expected bool
	}{
		{`"123"`, `"123"`, true},
		{`W/"123"`, `"123"`, false},
		{`"123"`, `W/"123"`, false},
		{`W/"123"`, `W/"123"`, false},
		{`"123"`, `"456"`, false},
		{"", `"123"`, false},
		{`"123"`, "", false},
	}

	for _, tt := range tests {
		got := strongETagMatches(tt.client, tt.cached)
		if got != tt.expected {
			t.Errorf("strongETagMatches(%q, %q) = %v, want %v", tt.client, tt.cached, got, tt.expected)
		}
	}
}
