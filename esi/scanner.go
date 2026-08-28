package esi

import (
	"bytes"
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
// It returns hasESI=true and the list of sorted EsiFragment descriptors if any ESI directives exist.
func Scan(b []byte) (hasESI bool, fragments []*proto.EsiFragment) {
	if len(b) == 0 {
		return false, nil
	}

	// Fast pre-check using SIMD bytes.Contains
	if !bytes.Contains(b, []byte("<esi:")) && !bytes.Contains(b, tagESIInlineComment) {
		return false, nil
	}

	fragments = make([]*proto.EsiFragment, 0, 8)
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
			stripStart := int32(tagStart)
			stripEnd := int32(tagStart + len(tagESIInlineComment))
			fragments = append(fragments, &proto.EsiFragment{
				StartPos: stripStart,
				EndPos:   stripEnd,
			})
			hasESI = true

			// Find matching --> closing wrapper
			contentStart := tagStart + len(tagESIInlineComment)
			closeIdx := bytes.Index(b[contentStart:], tagHTMLCommentClose)
			if closeIdx != -1 {
				commentCloseStart := contentStart + closeIdx
				commentCloseEnd := commentCloseStart + len(tagHTMLCommentClose)

				// Recursively scan inner block for tags within <!--esi ... -->
				innerBlock := b[contentStart:commentCloseStart]
				if innerHasESI, innerFrags := Scan(innerBlock); innerHasESI {
					for _, ifrag := range innerFrags {
						ifrag.StartPos += int32(contentStart)
						ifrag.EndPos += int32(contentStart)
						fragments = append(fragments, ifrag)
					}
				}

				// Strip --> closing wrapper
				fragments = append(fragments, &proto.EsiFragment{
					StartPos: int32(commentCloseStart),
					EndPos:   int32(commentCloseEnd),
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
			if isWhitespace(nextChar) || nextChar == '/' || nextChar == '>' {
				frag, nextPos := parseESIInclude(b, tagStart)
				if frag != nil {
					fragments = append(fragments, frag)
					hasESI = true
				}
				pos = nextPos
				continue
			}
		}

		// Check for <esi:remove>...</esi:remove>
		if tagStart+len(tagESIRemoveOpen) <= bufLen && bytes.Equal(b[tagStart:tagStart+len(tagESIRemoveOpen)], tagESIRemoveOpen) {
			nextChar := b[tagStart+len(tagESIRemoveOpen)]
			if isWhitespace(nextChar) || nextChar == '>' {
				closeIdx := bytes.Index(b[tagStart:], tagESIRemoveClose)
				if closeIdx != -1 {
					tagEnd := tagStart + closeIdx + len(tagESIRemoveClose)
					fragments = append(fragments, &proto.EsiFragment{
						StartPos: int32(tagStart),
						EndPos:   int32(tagEnd),
					})
					hasESI = true
					pos = tagEnd
					continue
				}
			}
		}

		// Check for <esi:comment ... /> or <esi:comment>...</esi:comment>
		if tagStart+len(tagESICommentOpen) <= bufLen && bytes.Equal(b[tagStart:tagStart+len(tagESICommentOpen)], tagESICommentOpen) {
			nextChar := b[tagStart+len(tagESICommentOpen)]
			if isWhitespace(nextChar) || nextChar == '/' || nextChar == '>' {
				closingBracket, isSelfClosing := findTagClosingBracket(b, tagStart)
				if closingBracket != -1 {
					if isSelfClosing {
						fragments = append(fragments, &proto.EsiFragment{
							StartPos: int32(tagStart),
							EndPos:   int32(closingBracket),
						})
						hasESI = true
						pos = closingBracket
						continue
					}
					// Paired comment tag
					closeIdx := bytes.Index(b[closingBracket:], tagESICommentClose)
					if closeIdx != -1 {
						tagEnd := closingBracket + closeIdx + len(tagESICommentClose)
						fragments = append(fragments, &proto.EsiFragment{
							StartPos: int32(tagStart),
							EndPos:   int32(tagEnd),
						})
						hasESI = true
						pos = tagEnd
						continue
					}
				}
			}
		}

		pos = tagStart + 1
	}

	return hasESI, fragments
}

// parseESIInclude parses an <esi:include> tag starting at tagStart.
func parseESIInclude(b []byte, tagStart int) (*proto.EsiFragment, int) {
	tagEndBracket, isSelfClosing := findTagClosingBracket(b, tagStart)
	if tagEndBracket == -1 {
		return nil, tagStart + 1
	}

	var tagHeader []byte
	if isSelfClosing {
		tagHeader = b[tagStart : tagEndBracket-2]
	} else {
		tagHeader = b[tagStart : tagEndBracket-1]
	}

	src := extractAttribute(tagHeader, attrSrc)
	alt := extractAttribute(tagHeader, attrAlt)
	timeoutBytes := extractAttributeBytes(tagHeader, attrTimeout)
	maxDepthBytes := extractAttributeBytes(tagHeader, attrMaxDepth)
	onError := extractAttribute(tagHeader, attrOnError)

	var timeoutMs uint32
	if len(timeoutBytes) > 0 {
		timeoutMs = parseTimeoutBytes(timeoutBytes)
	}

	var maxDepth uint32
	if len(maxDepthBytes) > 0 {
		maxDepth = uint32(parseUintBytes(maxDepthBytes))
	}

	if isSelfClosing {
		return &proto.EsiFragment{
			StartPos:  int32(tagStart),
			EndPos:    int32(tagEndBracket),
			Src:       src,
			Alt:       alt,
			OnError:   onError,
			MaxDepth:  maxDepth,
			TimeoutMs: timeoutMs,
		}, tagEndBracket
	}

	// Paired <esi:include>...</esi:include>
	closeIdx := bytes.Index(b[tagEndBracket:], tagESIIncludeClose)
	if closeIdx != -1 {
		bodyEnd := tagEndBracket + closeIdx
		fullEnd := bodyEnd + len(tagESIIncludeClose)
		fallbackBody := b[tagEndBracket:bodyEnd]

		return &proto.EsiFragment{
			StartPos:     int32(tagStart),
			EndPos:       int32(fullEnd),
			Src:          src,
			Alt:          alt,
			OnError:      onError,
			MaxDepth:     maxDepth,
			TimeoutMs:    timeoutMs,
			FallbackBody: bytes.Clone(fallbackBody),
		}, fullEnd
	}

	// Unclosed paired tag, treat as self-closing
	return &proto.EsiFragment{
		StartPos:  int32(tagStart),
		EndPos:    int32(tagEndBracket),
		Src:       src,
		Alt:       alt,
		OnError:   onError,
		MaxDepth:  maxDepth,
		TimeoutMs: timeoutMs,
	}, tagEndBracket
}

// findTagClosingBracket locates the end of an opening HTML/ESI tag, respecting quoted strings.
// Returns the slice index after the closing '>' and whether the tag ends with '/>'.
func findTagClosingBracket(b []byte, tagStart int) (int, bool) {
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

// extractAttributeBytes scans a tag header for attr="value" or attr='value' or attr=value and returns []byte.
func extractAttributeBytes(tagHeader []byte, attrName []byte) []byte {
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
		validBefore := (attrStart == 0) || isWhitespace(tagHeader[attrStart-1])
		afterIdx := attrStart + len(attrName)
		if !validBefore || afterIdx >= headerLen {
			idx = afterIdx
			continue
		}

		// Skip whitespace between attr name and '='
		curr := afterIdx
		for curr < headerLen && isWhitespace(tagHeader[curr]) {
			curr++
		}

		if curr >= headerLen || tagHeader[curr] != '=' {
			idx = afterIdx
			continue
		}

		// Skip '=' and whitespace
		curr++
		for curr < headerLen && isWhitespace(tagHeader[curr]) {
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
		for curr < headerLen && !isWhitespace(tagHeader[curr]) && tagHeader[curr] != '/' && tagHeader[curr] != '>' {
			curr++
		}
		return tagHeader[valStart:curr]
	}

	return nil
}

// extractAttribute scans a tag header for attr="value" or attr='value' or attr=value.
func extractAttribute(tagHeader []byte, attrName []byte) string {
	b := extractAttributeBytes(tagHeader, attrName)
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

// parseTimeoutBytes parses timeout strings like "0.5", "2.5s", "500ms" into milliseconds.
// ponytail: stdlib time.ParseDuration covers this; fallback bare seconds → ms.
func parseTimeoutBytes(b []byte) uint32 {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return 0
	}
	s := string(b)
	// time.ParseDuration requires unit; bare number means seconds per ESI spec
	if d, err := time.ParseDuration(s); err == nil {
		return uint32(d.Milliseconds())
	}
	if d, err := time.ParseDuration(s + "s"); err == nil {
		return uint32(d.Milliseconds())
	}
	return 0
}

// parseUintBytes parses a uint64 from []byte (used for max-depth).
func parseUintBytes(b []byte) uint64 {
	b = bytes.TrimSpace(b)
	var val uint64
	for _, c := range b {
		if c >= '0' && c <= '9' {
			val = val*10 + uint64(c-'0')
		} else {
			break
		}
	}
	return val
}

func isWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
