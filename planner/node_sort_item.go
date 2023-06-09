package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/row"
)

type SortItemNode struct {
	Expression *ExpressionNode
	OrderType  datatype.OrderType
}

func NewSortItemNode(runtime *config.Runtime, t parser.ISortItemContext) *SortItemNode {
	tt := t.(*parser.SortItemContext)
	res := &SortItemNode{
		Expression: NewExpressionNode(runtime, tt.Expression()),
		OrderType:  datatype.ASC,
	}

	if ot := tt.GetOrdering(); ot != nil {
		if ot.GetText() != "ASC" {
			res.OrderType = datatype.DESC
		}
	}

	return res
}

func (n *SortItemNode) GetColumns() ([]string, error) {
	return n.Expression.GetColumns()
}

func (n *SortItemNode) Init(md *metadata.Metadata) error {
	return n.Expression.Init(md)
}

func (n *SortItemNode) Result(input *row.RowsGroup) (interface{}, error) {
	return n.Expression.Result(input)
}

func (n *SortItemNode) IsAggregate() bool {
	return n.Expression.IsAggregate()
}

func (n *SortItemNode) GetType(md *metadata.Metadata) (datatype.Type, error) {
	return n.Expression.GetType(md)
}
