package stage

import (
	"container/heap"
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/util"
	"testing"
)

func TestStage(t *testing.T) {
	config.Load("../config.yaml")
	sqlStr := "select * from file.info.student where id = 1 and options = '{ \"age\":30 }'"
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
	logicalTree := plan.NewNodeFromSingleStatement(config.NewRuntime(), tree)

	//SetMetaData
	if err := logicalTree.SetMetadata(); err != nil {
		t.Error(err)
		return
	}

	executorHeap := util.NewHeap()
	heap.Init(executorHeap)
	for i := 0; i < 100; i++ {
		heap.Push(executorHeap, util.NewItem(&pb.Location{Name: fmt.Sprintf("%v", i)}, 1))
	}

	jobs, err := CreateJob(logicalTree, executorHeap, 4)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("%+v", &jobs)
	t.Logf("%+v", &logicalTree)
}
