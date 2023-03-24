package operator

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type PredicateNode struct {
	ComparisonOperator   *gtype.Operator
	RightValueExpression *ValueExpressionNode
}

func NewPredicateNode(runtime *config.Runtime, t parser.IPredicateContext) *PredicateNode {
	tt := t.(*parser.PredicateContext)
	res := &PredicateNode{}
	if iopc, ve := tt.ComparisonOperator(), tt.GetRight(); iopc != nil && ve != nil {
		res.ComparisonOperator = NewComparisonOperator(runtime, iopc)
		res.RightValueExpression = NewValueExpressionNode(runtime, ve)
	}
	return res
}

func (n *PredicateNode) GetType(_ *metadata.Metadata) (gtype.Type, error) {
	return gtype.BOOL, nil
}

func (n *PredicateNode) ExtractAggFunc(res *[]*FuncCallNode) {
	n.RightValueExpression.ExtractAggFunc(res)
}

func (n *PredicateNode) GetColumns() ([]string, error) {
	return n.RightValueExpression.GetColumns()
}

func (n *PredicateNode) Init(md *metadata.Metadata) error {
	if n.RightValueExpression != nil {
		return n.RightValueExpression.Init(md)

	}
	return nil
}

func (n *PredicateNode) Result(valsi interface{}, input *row.RowsGroup) (interface{}, error) {
	if n.ComparisonOperator != nil && n.RightValueExpression != nil {
		resi, err := n.RightValueExpression.Result(input)
		if err != nil {
			return nil, err
		}
		vals, res := valsi.([]interface{}), resi.([]interface{})
		for i := 0; i < len(res); i++ {
			res[i] = gtype.OperatorFunc(vals[i], res[i], *n.ComparisonOperator)
		}
		return res, nil
	} else {
		return false, fmt.Errorf("wrong PredicateNode")
	}
}
