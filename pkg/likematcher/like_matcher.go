package likematcher

import (
	"errors"
	"strings"
)

type LikeMatcher struct {
	Pattern string
	Escape  string
	MinSize int
	MaxSize int
	Prefix  []byte
	Suffix  []byte
	Matcher Matcher
}

type Matcher interface {
	Match(input []byte, offset int, length int) bool
}

func Parse(pattern string, escape string) ([]string, error) {
	var result []string
	literal := ""
	anyCount := 0
	anyUnbounded := false
	inEscape := false
	for i := 0; i < len(pattern); i++ {
		character := string(pattern[i])

		if inEscape {
			if character != "%" && character != "_" && strings.Contains(escape, character) {
				return result, errors.New("escape character must be followed by '%', '_' or the escape character itself")
			}
			literal += character
			inEscape = false
		} else if escape != "" && strings.Contains(escape, character) {
			inEscape = true
			if anyCount != 0 {
				result = append(result, strings.Repeat("_", anyCount))
				anyCount = 0
			}

			if anyUnbounded {
				result = append(result, "%")
				anyUnbounded = false
			}
		} else if character == "%" || character == "_" {
			if len(literal) != 0 {
				result = append(result, literal)
				literal = ""
			}

			if character == "%" {
				anyUnbounded = true
			} else {
				anyCount++
			}
		} else {
			if anyCount != 0 {
				result = append(result, strings.Repeat("_", anyCount))
				anyCount = 0
			}

			if anyUnbounded {
				result = append(result, "%")
				anyUnbounded = false
			}

			literal += character
		}

	}

	if inEscape {
		return result, errors.New("escape character must be followed by '%', '_' or the escape character itself")
	}

	if len(literal) != 0 {
		result = append(result, literal)
	} else {
		if anyCount != 0 {
			result = append(result, strings.Repeat("_", anyCount))
		}

		if anyUnbounded {
			result = append(result, "%")
		}
	}

	return result, nil
}

func Compile(pattern string, escape string) (*LikeMatcher, error) {
	parsed, err := Parse(pattern, escape)
	if err != nil {
		return nil, err
	}
	// Calculate minimum and maximum size for candidate strings
	// This is used for short-circuiting the match if the size of
	// the input is outside those bounds
	minSize := 0
	maxSize := 0
	unbounded := false
	for _, expression := range parsed {
		if isZeroOrMore(expression) {
			unbounded = true
		} else if isAny(expression) {
			length := len(expression)
			minSize += length
			maxSize += length * 4 // at most 4 bytes for a single UTF-8 codepoint
		} else {
			length := len(expression)
			minSize += length
			maxSize += length
		}
	}

	// Calculate exact match prefix and suffix
	// If the pattern starts and ends with a literal,
	// we can perform a quick exact match to short-circuit DFA evaluation
	var prefix, suffix []byte
	patternStart := 0
	patternEnd := len(parsed) - 1

	if len(parsed) > 0 && isLiteral(parsed[0]) {
		prefix = []byte(parsed[0])
		patternStart++
	}

	if len(parsed) > 1 && isLiteral(parsed[len(parsed)-1]) {
		suffix = []byte(parsed[len(parsed)-1])
		patternEnd--
	}

	exact := true
	if patternStart <= patternEnd && parsed[patternEnd] == "%" {
		// guaranteed to be Any or ZeroOrMore because any Literal would've been turned into a suffix above
		exact = false
		patternEnd--
	}

	var matcher Matcher
	if patternStart <= patternEnd {
		matcher = NewNfaMatcher(parsed, patternStart, patternEnd, exact)
	}

	if unbounded {
		maxSize = -1
	}
	return &LikeMatcher{
		Pattern: pattern,
		Escape:  escape,
		MinSize: minSize,
		MaxSize: maxSize,
		Prefix:  prefix,
		Suffix:  suffix,
		Matcher: matcher,
	}, nil
}

func (m *LikeMatcher) Match(input []byte) bool {
	return m.match(input, 0, len(input))
}

func (m *LikeMatcher) match(input []byte, offset int, length int) bool {
	if length < m.MinSize {
		return false
	}

	if m.MaxSize != -1 && length > m.MaxSize {
		return false
	}

	if !startsWith(m.Prefix, input, offset) {
		return false
	}

	if !startsWith(m.Suffix, input, offset+length-len(m.Suffix)) {
		return false
	}

	if m.Matcher != nil {
		return m.Matcher.Match(input, offset+len(m.Prefix), length-len(m.Suffix)-len(m.Prefix))
	}

	return true
}

func startsWith(pattern []byte, input []byte, offset int) bool {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != input[offset+i] {
			return false
		}
	}

	return true
}

func isLiteral(str string) bool {
	return str != "%" && !strings.Contains(str, "_")
}

func isAny(str string) bool {
	return strings.Contains(str, "_")
}

func isZeroOrMore(str string) bool {
	return str == "%"
}
