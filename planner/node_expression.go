package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/row"
)

type ExpressionNode struct {
	Name              string
	BooleanExpression *BooleanExpressionNode
}

func NewExpressionNode(runtime *config.Runtime, t parser.IExpressionContext) *ExpressionNode {
	tt := t.(*parser.ExpressionContext)
	res := &ExpressionNode{
		Name:              "",
		BooleanExpression: NewBooleanExpressionNode(runtime, tt.BooleanExpression()),
	}
	res.Name = res.BooleanExpression.Name
	return res
}

func (n *ExpressionNode) ExtractAggFunc(res *[]*FuncCallNode) {
	n.BooleanExpression.ExtractAggFunc(res)
}

func (n *ExpressionNode) GetType(md *metadata.Metadata) (datatype.Type, error) {
	return n.BooleanExpression.GetType(md)
}

func (n *ExpressionNode) GetColumns() ([]string, error) {
	return n.BooleanExpression.GetColumns()
}

func (n *ExpressionNode) Init(md *metadata.Metadata) error {
	return n.BooleanExpression.Init(md)
}

func (n *ExpressionNode) Result(input *row.RowsGroup) (interface{}, error) {
	return n.BooleanExpression.Result(input)
}

func (n *ExpressionNode) IsAggregate() bool {
	return n.BooleanExpression.IsAggregate()
}
