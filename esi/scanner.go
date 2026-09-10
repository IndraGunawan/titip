package esi

import (
	"bytes"
	"strconv"
	"time"

	proto "github.com/indragunawan/titip/proto"
)

var (
	tagESIIncludeOpen   = []byte("<esi:include")
	tagESIIncludeClose  = []byte("</esi:include>")
	tagESIRemoveOpen    = []byte("<esi:remove")
	tagESIRemoveClose   = []byte("</esi:remove>")
	tagESICommentOpen   = []byte("<esi:comment")
	tagESICommentClose  = []byte("</esi:comment>")
	tagESIInlineComment = []byte("<!--esi")
	tagHTMLCommentClose = []byte("-->")

	attrSrc      = []byte("src")
	attrAlt      = []byte("alt")
	attrTimeout  = []byte("timeout")
	attrMaxDepth = []byte("max-depth")
	attrOnError  = []byte("onerror")
)

// Scan inspects the HTML byte slice for ESI tags and extracts pre-compiled fragment metadata.
// It returns a slice of EsiFragment descriptors if any ESI directives exist, or nil if none are found.
func Scan(b []byte) []*proto.EsiFragment {
	if len(b) == 0 {
		return nil
	}

	// Fast pre-check using SIMD bytes.Contains
	if !bytes.Contains(b, []byte("<esi:")) && !bytes.Contains(b, tagESIInlineComment) {
		return nil
	}

	var fragments []*proto.EsiFragment
	pos := 0
	bufLen := len(b)

	for pos < bufLen {
		// Fast-forward to next '<'
		nextIdx := bytes.IndexByte(b[pos:], '<')
		if nextIdx == -1 {
			break
		}
		tagStart := pos + nextIdx

		// Check for <!--esi unescaping
		if tagStart+len(tagESIInlineComment) <= bufLen && bytes.Equal(b[tagStart:tagStart+len(tagESIInlineComment)], tagESIInlineComment) {
			// Strip <!--esi opening wrapper
			stripStart := int64(tagStart)
			stripEnd := int64(tagStart + len(tagESIInlineComment))
			fragments = append(fragments, &proto.EsiFragment{
				StartPos: stripStart,
				EndPos:   stripEnd,
			})

			// Find matching --> closing wrapper
			contentStart := tagStart + len(tagESIInlineComment)
			closeIdx := bytes.Index(b[contentStart:], tagHTMLCommentClose)
			if closeIdx != -1 {
				commentCloseStart := contentStart + closeIdx
				commentCloseEnd := commentCloseStart + len(tagHTMLCommentClose)

				// Recursively scan inner block for tags within <!--esi ... -->
				innerBlock := b[contentStart:commentCloseStart]
				for _, ifrag := range Scan(innerBlock) {
					ifrag.StartPos += int64(contentStart)
					ifrag.EndPos += int64(contentStart)
					if ifrag.InnerStartPos > 0 {
						ifrag.InnerStartPos += int64(contentStart)
						ifrag.InnerEndPos += int64(contentStart)
					}
					fragments = append(fragments, ifrag)
				}

				// Strip --> closing wrapper
				fragments = append(fragments, &proto.EsiFragment{
					StartPos: int64(commentCloseStart),
					EndPos:   int64(commentCloseEnd),
				})

				pos = commentCloseEnd
				continue
			}

			pos = tagStart + len(tagESIInlineComment)
			continue
		}

		// Check for <esi:include
		if tagStart+len(tagESIIncludeOpen) <= bufLen && bytes.Equal(b[tagStart:tagStart+len(tagESIIncludeOpen)], tagESIIncludeOpen) {
			// Must have a whitespace or '/' or '>' right after tag name
			nextChar := b[tagStart+len(tagESIIncludeOpen)]
			if isASCIIWhitespace(nextChar) || nextChar == '/' || nextChar == '>' {
				frag, nextPos := parseESIInclude(b, tagStart)
				if frag != nil {
					fragments = append(fragments, frag)
				}
				pos = nextPos
				continue
			}
		}

		// Check for <esi:remove>...</esi:remove>
		if tagStart+len(tagESIRemoveOpen) <= bufLen && bytes.Equal(b[tagStart:tagStart+len(tagESIRemoveOpen)], tagESIRemoveOpen) {
			nextChar := b[tagStart+len(tagESIRemoveOpen)]
			if isASCIIWhitespace(nextChar) || nextChar == '>' {
				closeIdx := bytes.Index(b[tagStart:], tagESIRemoveClose)
				if closeIdx != -1 {
					tagEnd := tagStart + closeIdx + len(tagESIRemoveClose)
					fragments = append(fragments, &proto.EsiFragment{
						StartPos: int64(tagStart),
						EndPos:   int64(tagEnd),
					})
					pos = tagEnd
					continue
				}
			}
		}

		// Check for <esi:comment ... /> or <esi:comment>...</esi:comment>
		if tagStart+len(tagESICommentOpen) <= bufLen && bytes.Equal(b[tagStart:tagStart+len(tagESICommentOpen)], tagESICommentOpen) {
			nextChar := b[tagStart+len(tagESICommentOpen)]
			if isASCIIWhitespace(nextChar) || nextChar == '/' || nextChar == '>' {
				endOffset, isSelfClosing := scanTagHeaderEnd(b, tagStart)
				if endOffset != -1 {
					if isSelfClosing {
						fragments = append(fragments, &proto.EsiFragment{
							StartPos: int64(tagStart),
							EndPos:   int64(endOffset),
						})
						pos = endOffset
						continue
					}
					// Paired comment tag
					closeIdx := bytes.Index(b[endOffset:], tagESICommentClose)
					if closeIdx != -1 {
						tagEnd := endOffset + closeIdx + len(tagESICommentClose)
						fragments = append(fragments, &proto.EsiFragment{
							StartPos: int64(tagStart),
							EndPos:   int64(tagEnd),
						})
						pos = tagEnd
						continue
					}
				}
			}
		}

		pos = tagStart + 1
	}

	return fragments
}

// parseESIInclude parses an <esi:include> tag starting at tagStart.
func parseESIInclude(b []byte, tagStart int) (*proto.EsiFragment, int) {
	tagEndOffset, isSelfClosing := scanTagHeaderEnd(b, tagStart)
	if tagEndOffset == -1 {
		return nil, tagStart + 1
	}

	var tagHeader []byte
	if isSelfClosing {
		tagHeader = b[tagStart : tagEndOffset-2]
	} else {
		tagHeader = b[tagStart : tagEndOffset-1]
	}

	src := extractAttribute(tagHeader, attrSrc)
	alt := extractAttribute(tagHeader, attrAlt)
	timeoutRaw := extractAttributeRaw(tagHeader, attrTimeout)
	maxDepthRaw := extractAttributeRaw(tagHeader, attrMaxDepth)
	onError := extractAttribute(tagHeader, attrOnError)

	var timeoutMs int64
	if len(timeoutRaw) > 0 {
		timeoutMs = parseTimeout(timeoutRaw)
	}

	var maxDepth uint32
	if len(maxDepthRaw) > 0 {
		if v, err := strconv.ParseUint(string(bytes.TrimSpace(maxDepthRaw)), 10, 32); err == nil {
			maxDepth = uint32(v)
		}
	}

	if isSelfClosing {
		return &proto.EsiFragment{
			StartPos:  int64(tagStart),
			EndPos:    int64(tagEndOffset),
			Src:       src,
			Alt:       alt,
			OnError:   onError,
			MaxDepth:  maxDepth,
			TimeoutMs: timeoutMs,
		}, tagEndOffset
	}

	// Paired <esi:include>...</esi:include>
	closeIdx := bytes.Index(b[tagEndOffset:], tagESIIncludeClose)
	if closeIdx != -1 {
		bodyEnd := tagEndOffset + closeIdx
		fullEnd := bodyEnd + len(tagESIIncludeClose)

		return &proto.EsiFragment{
			StartPos:      int64(tagStart),
			EndPos:        int64(fullEnd),
			Src:           src,
			Alt:           alt,
			OnError:       onError,
			MaxDepth:      maxDepth,
			TimeoutMs:     timeoutMs,
			InnerStartPos: int64(tagEndOffset),
			InnerEndPos:   int64(bodyEnd),
		}, fullEnd
	}

	// Unclosed paired tag, treat as self-closing
	return &proto.EsiFragment{
		StartPos:  int64(tagStart),
		EndPos:    int64(tagEndOffset),
		Src:       src,
		Alt:       alt,
		OnError:   onError,
		MaxDepth:  maxDepth,
		TimeoutMs: timeoutMs,
	}, tagEndOffset
}

// scanTagHeaderEnd locates the end of an opening HTML/ESI tag, respecting quoted strings.
// Returns the slice offset immediately after the closing '>' and whether the tag was self-closing ('/>').
// If unclosed, endOffset returns -1.
func scanTagHeaderEnd(b []byte, tagStart int) (endOffset int, isSelfClosing bool) {
	bufLen := len(b)
	i := tagStart + 1
	var inQuote byte

	for i < bufLen {
		c := b[i]

		if inQuote != 0 {
			if c == inQuote {
				inQuote = 0
			}
			i++
			continue
		}

		if c == '"' || c == '\'' {
			inQuote = c
			i++
			continue
		}

		if c == '>' {
			isSelfClosing := false
			if i > 0 && b[i-1] == '/' {
				isSelfClosing = true
			}
			return i + 1, isSelfClosing
		}

		i++
	}

	return -1, false
}

// extractAttributeRaw scans a tag header for attr="value" or attr='value' or attr=value and returns []byte without allocating.
func extractAttributeRaw(tagHeader []byte, attrName []byte) []byte {
	idx := 0
	headerLen := len(tagHeader)

	for idx < headerLen {
		// Find attribute name
		matchIdx := bytes.Index(tagHeader[idx:], attrName)
		if matchIdx == -1 {
			return nil
		}
		attrStart := idx + matchIdx

		// Check boundaries before and after attribute name
		validBefore := (attrStart == 0) || isASCIIWhitespace(tagHeader[attrStart-1])
		afterIdx := attrStart + len(attrName)
		if !validBefore || afterIdx >= headerLen {
			idx = afterIdx
			continue
		}

		// Skip whitespace between attr name and '='
		curr := afterIdx
		for curr < headerLen && isASCIIWhitespace(tagHeader[curr]) {
			curr++
		}

		if curr >= headerLen || tagHeader[curr] != '=' {
			idx = afterIdx
			continue
		}

		// Skip '=' and whitespace
		curr++
		for curr < headerLen && isASCIIWhitespace(tagHeader[curr]) {
			curr++
		}

		if curr >= headerLen {
			return nil
		}

		// Read value
		quote := tagHeader[curr]
		if quote == '"' || quote == '\'' {
			valStart := curr + 1
			valEndRel := bytes.IndexByte(tagHeader[valStart:], quote)
			if valEndRel == -1 {
				return tagHeader[valStart:]
			}
			return tagHeader[valStart : valStart+valEndRel]
		}

		// Unquoted value
		valStart := curr
		for curr < headerLen && !isASCIIWhitespace(tagHeader[curr]) && tagHeader[curr] != '>' {
			if tagHeader[curr] == '/' && (curr+1 >= headerLen || tagHeader[curr+1] == '>') {
				break
			}
			curr++
		}
		return tagHeader[valStart:curr]
	}

	return nil
}

// extractAttribute scans a tag header for attr="value" or attr='value' or attr=value.
func extractAttribute(tagHeader []byte, attrName []byte) string {
	b := extractAttributeRaw(tagHeader, attrName)
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

// parseTimeout parses timeout strings like "0.5", "2.5s", "500ms" into milliseconds.
// stdlib time.ParseDuration covers this; fallback bare seconds → ms.
func parseTimeout(b []byte) int64 {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return 0
	}
	s := string(b)
	// time.ParseDuration requires unit; bare number means seconds per ESI spec
	if d, err := time.ParseDuration(s); err == nil {
		if d <= 0 {
			return 0
		}
		return d.Milliseconds()
	}
	if d, err := time.ParseDuration(s + "s"); err == nil {
		if d <= 0 {
			return 0
		}
		return d.Milliseconds()
	}
	return 0
}

// isASCIIWhitespace reports whether c is an ASCII whitespace byte (space, tab, LF, or CR).
// HTML5 §2.4.1 and XML 1.0 §2.3 strictly restrict tag whitespace to ASCII whitespace.
func isASCIIWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
