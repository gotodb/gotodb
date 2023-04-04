package plan

import (
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/parser"
	"testing"
)

func TestPlan(t *testing.T) {
	sqlStr := "select * from test.test.csv"
	inputStream := antlr.NewInputStream(sqlStr)
	lexer := parser.NewSqlLexer(parser.NewCaseChangingStream(inputStream, true))
	p := parser.NewSqlParser(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
	errListener := parser.NewErrorListener()
	p.AddErrorListener(errListener)
	tree := p.SingleStatement()
	if errListener.HasError() {
		t.Error(errListener)
		return
	}
	logicalTree := NewNodeFromSingleStatement(config.NewRuntime(), tree)

	//SetMetaData
	if err := logicalTree.SetMetadata(); err != nil {
		t.Error(err)
		return
	}

	t.Logf("%+v", &logicalTree)

}
