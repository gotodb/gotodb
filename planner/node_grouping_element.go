package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/row"
)

type GroupingElementNode struct {
	Expression *ExpressionNode
}

func NewGroupingElementNode(runtime *config.Runtime, t parser.IGroupingElementContext) *GroupingElementNode {
	res := &GroupingElementNode{}
	tt := t.(*parser.GroupingElementContext).Expression()
	res.Expression = NewExpressionNode(runtime, tt)
	return res
}

func (n *GroupingElementNode) Init(md *metadata.Metadata) error {
	return n.Expression.Init(md)
}

func (n *GroupingElementNode) Result(input *row.RowsGroup) (interface{}, error) {
	return n.Expression.Result(input)
}

func (n *GroupingElementNode) GetColumns() ([]string, error) {
	return n.Expression.GetColumns()
}

func (n *GroupingElementNode) GetType(md *metadata.Metadata) (datatype.Type, error) {
	return n.Expression.GetType(md)
}

func (n *GroupingElementNode) IsAggregate() bool {
	return n.Expression.IsAggregate()
}
