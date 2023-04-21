package operator

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/row"
)

type PredicateNode struct {
	ComparisonOperator   *gtype.Operator
	IsNot                bool
	RightValueExpression *ValueExpressionNode
	LowerValueExpression *ValueExpressionNode
	UpperValueExpression *ValueExpressionNode
}

func NewPredicateNode(runtime *config.Runtime, t parser.IPredicateContext) *PredicateNode {
	tt := t.(*parser.PredicateContext)
	res := &PredicateNode{}
	if iopc, ve := tt.ComparisonOperator(), tt.GetRight(); iopc != nil && ve != nil {
		res.ComparisonOperator = NewComparisonOperator(runtime, iopc)
		res.RightValueExpression = NewValueExpressionNode(runtime, ve)
	} else if tt.BETWEEN() != nil {
		if tt.NOT() != nil {
			res.IsNot = true
		}
		res.LowerValueExpression = NewValueExpressionNode(runtime, tt.GetLower())
		res.UpperValueExpression = NewValueExpressionNode(runtime, tt.GetUpper())
	}
	return res
}

func (n *PredicateNode) GetType(_ *metadata.Metadata) (gtype.Type, error) {
	return gtype.BOOL, nil
}

func (n *PredicateNode) ExtractAggFunc(res *[]*FuncCallNode) {
	if n.RightValueExpression != nil {
		n.RightValueExpression.ExtractAggFunc(res)
	} else if n.LowerValueExpression != nil || n.UpperValueExpression != nil {
		n.LowerValueExpression.ExtractAggFunc(res)
		n.UpperValueExpression.ExtractAggFunc(res)
	}
}

func (n *PredicateNode) GetColumns() ([]string, error) {
	if n.RightValueExpression != nil {
		return n.RightValueExpression.GetColumns()
	} else if n.LowerValueExpression != nil || n.UpperValueExpression != nil {
		l, err := n.LowerValueExpression.GetColumns()
		if err != nil {
			return nil, err
		}
		u, err := n.UpperValueExpression.GetColumns()
		if err != nil {
			return nil, err
		}

		return append(l, u...), nil
	} else {
		return []string{}, fmt.Errorf("predicate get columns error")
	}
}

func (n *PredicateNode) Init(md *metadata.Metadata) error {
	if n.RightValueExpression != nil {
		return n.RightValueExpression.Init(md)
	}

	if n.LowerValueExpression != nil && n.UpperValueExpression != nil {
		if err := n.LowerValueExpression.Init(md); err != nil {
			return err
		}
		if err := n.UpperValueExpression.Init(md); err != nil {
			return err
		}
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
	} else if n.LowerValueExpression != nil && n.UpperValueExpression != nil {
		loweri, err := n.LowerValueExpression.Result(input)
		if err != nil {
			return nil, err
		}

		upperi, err := n.UpperValueExpression.Result(input)
		if err != nil {
			return nil, err
		}

		vals, lower, upper := valsi.([]interface{}), loweri.([]interface{}), upperi.([]interface{})

		res := make([]interface{}, len(vals))

		for i := 0; i < len(res); i++ {
			if n.IsNot {
				res[i] = gtype.OperatorFunc(vals[i], lower[i], gtype.LT).(bool) || gtype.OperatorFunc(vals[i], upper[i], gtype.GT).(bool)
			} else {
				res[i] = gtype.OperatorFunc(vals[i], lower[i], gtype.GTE).(bool) && gtype.OperatorFunc(vals[i], upper[i], gtype.LTE).(bool)
			}
		}
		return res, nil
	} else {
		return false, fmt.Errorf("wrong PredicateNode")
	}
}
