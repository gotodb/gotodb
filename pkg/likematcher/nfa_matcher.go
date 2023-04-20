package likematcher

type NfaMatcher struct {
	Exact       bool
	LoopBack    []bool
	Matches     []int
	AcceptState int
	StateCount  int
}

const (
	Any              = -1
	None             = -2
	InvalidCodepoint = -1
)

func NewNfaMatcher(pattern []string, start, end int, exact bool) *NfaMatcher {
	stateCount := calculateStateCount(pattern, start, end)
	loopBack := make([]bool, stateCount)
	matches := make([]int, stateCount)
	for i := range matches {
		matches[i] = None
	}
	state := 0
	for j := start; j <= end; j++ {
		element := pattern[j]
		if isLiteral(element) {
			for _, literal := range element {
				matches[state] = int(literal)
				state++
			}
		} else if isAny(element) {
			for i := 0; i < len(element); i++ {
				matches[state] = Any
				state++
			}
		} else if isZeroOrMore(element) {
			loopBack[state] = true
		}
	}
	return &NfaMatcher{
		Exact:       exact,
		LoopBack:    loopBack,
		Matches:     matches,
		AcceptState: stateCount - 1,
		StateCount:  stateCount,
	}
}

func calculateStateCount(pattern []string, start, end int) int {
	states := 1
	for i := start; i <= end; i++ {
		element := pattern[i]
		if isLiteral(element) {
			states += len(element)
		} else if isAny(element) {
			states += len(element)
		}
	}
	return states
}

func (matcher NfaMatcher) Match(input []byte, offset int, length int) bool {
	seen := make([]bool, matcher.StateCount+1)
	currentStates := make([]int, matcher.StateCount)
	nextStates := make([]int, matcher.StateCount)
	currentStatesIndex := 1
	nextStatesIndex := 0
	limit := offset + length
	current := offset
	accept := false

	for current < limit {
		codepoint := InvalidCodepoint
		header := int(input[current] & 0xff)
		if header < 0x80 {
			// normal ASCII
			// 0xxx_xxxx
			codepoint = header
			current++
		} else if (header & 0b1110_0000) == 0b1100_0000 {
			// 110x_xxxx 10xx_xxxx
			if current+1 < limit {
				codepoint = ((header & 0b0001_1111) << 6) | (int(input[current+1] & 0b0011_1111))
				current += 2
			}
		} else if (header & 0b1111_0000) == 0b1110_0000 {
			// 1110_xxxx 10xx_xxxx 10xx_xxxx
			if current+2 < limit {
				codepoint = ((header & 0b0000_1111) << 12) | (int(input[current+1]&0b0011_1111) << 6) | (int(input[current+2] & 0b0011_1111))
				current += 3
			}
		} else if (header & 0b1111_1000) == 0b1111_0000 {
			// 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx
			if current+3 < limit {
				codepoint = ((header & 0b0000_0111) << 18) | (int(input[current+1]&0b0011_1111) << 12) | (int(input[current+2]&0b0011_1111) << 6) | (int(input[current+3] & 0b0011_1111))
				current += 4
			}
		}

		if codepoint == InvalidCodepoint {
			return false
		}

		accept = false
		nextStatesIndex = 0
		for i := range seen {
			seen[i] = false
		}

		for i := 0; i < currentStatesIndex; i++ {
			state := currentStates[i]
			if !seen[state] && matcher.LoopBack[state] {
				nextStates[nextStatesIndex] = state
				nextStatesIndex++
				if !accept {
					accept = state == matcher.AcceptState
				}
				seen[state] = true
			}
			next := state + 1
			if !seen[next] && (matcher.Matches[state] == Any || matcher.Matches[state] == codepoint) {
				nextStates[nextStatesIndex] = next
				nextStatesIndex++
				if !accept {
					accept = next == matcher.AcceptState
				}
				seen[next] = true
			}
		}

		if nextStatesIndex == 0 {
			return false
		}

		if !matcher.Exact && accept {
			return true
		}

		tmp := currentStates
		currentStates = nextStates
		nextStates = tmp
		currentStatesIndex = nextStatesIndex
	}
	return accept
}
