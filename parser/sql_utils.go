package parser

import (
	"fmt"
	"strings"

	. "github.com/antlr/antlr4/runtime/Go/antlr/v4"
)

type MyErrorListener struct {
	*DefaultErrorListener
	Msgs []string
}

func NewErrorListener() *MyErrorListener {
	return new(MyErrorListener)
}

func (listener *MyErrorListener) SyntaxError(recognizer Recognizer, offendingSymbol interface{}, line, column int, msg string, e RecognitionException) {
	listener.Msgs = append(listener.Msgs, fmt.Sprintf("line %v:%v  ", line, column)+msg)
}

func (listener *MyErrorListener) HasError() bool {
	return len(listener.Msgs) > 0
}

func (listener *MyErrorListener) String() string {
	return strings.Join(listener.Msgs, "\n")
}
