package parser

import (
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"testing"
)

func TestShowCatalogs(t *testing.T) {
	sqlStr := "SHOW CATALOGS"
	inputStream := antlr.NewInputStream(sqlStr)
	lexer := NewSqlLexer(NewCaseChangingStream(inputStream, true))
	p := NewSqlParser(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
	errListener := NewErrorListener()
	p.AddErrorListener(errListener)
	tree := p.SingleStatement()
	if errListener.HasError() {
		t.Error(errListener)
	} else {
		t.Logf("%+v", &tree)
	}
}

func TestShowTables(t *testing.T) {
	sqlStr := "SHOW TABLES"
	inputStream := antlr.NewInputStream(sqlStr)
	lexer := NewSqlLexer(NewCaseChangingStream(inputStream, true))
	p := NewSqlParser(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
	errListener := NewErrorListener()
	p.AddErrorListener(errListener)
	tree := p.SingleStatement()
	if errListener.HasError() {
		t.Error(errListener)
	} else {
		t.Logf("%+v", tree)
	}
}
