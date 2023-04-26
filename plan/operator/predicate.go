package operator

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/likematcher"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/row"
)

type PredicateNode struct {
	Type                   PredicateType
	ComparisonOperator     *datatype.Operator
	IsNot                  bool
	FirstValueExpression   *ValueExpressionNode
	SecondValueExpression  *ValueExpressionNode
	BooleanExpressionNodes []*BooleanExpressionNode
}

type PredicateType int

const (
	PredicateTypeUnknown PredicateType = iota
	PredicateTypeComparison
	PredicateTypeComparisonQuery
	PredicateTypeBetween
	PredicateTypeIn
	PredicateTypeInQuery
	PredicateTypeLike
	PredicateTypeDistinctFrom
	PredicateTypeIsNull
)

func NewPredicateNode(runtime *config.Runtime, t parser.IPredicateContext) *PredicateNode {
	tt := t.(*parser.PredicateContext)
	res := &PredicateNode{
		Type: PredicateTypeUnknown,
	}
	if tt.NOT() != nil {
		res.IsNot = true
	}
	if ve := tt.GetRight(); ve != nil {
		res.FirstValueExpression = NewValueExpressionNode(runtime, ve)
		// comparisonOperator right=valueExpression
		if iopc := tt.ComparisonOperator(); iopc != nil {
			res.Type = PredicateTypeComparison
			res.ComparisonOperator = NewComparisonOperator(runtime, iopc)
		} else {
			// IS NOT? DISTINCT FROM right=valueExpression
			res.Type = PredicateTypeDistinctFrom
			if res.IsNot {
				op := datatype.EQ
				res.ComparisonOperator = &op
			} else {
				op := datatype.NEQ
				res.ComparisonOperator = &op
			}
		}

	} else if tt.BETWEEN() != nil {
		// NOT? BETWEEN lower=valueExpression AND upper=valueExpression
		res.Type = PredicateTypeBetween
		res.FirstValueExpression = NewValueExpressionNode(runtime, tt.GetLower())
		res.SecondValueExpression = NewValueExpressionNode(runtime, tt.GetUpper())
	} else if tt.IN() != nil {
		// NOT? IN '(' expression (',' expression)* ')'
		res.Type = PredicateTypeIn
		for _, exp := range tt.AllExpression() {
			res.BooleanExpressionNodes = append(res.BooleanExpressionNodes, NewBooleanExpressionNode(runtime, exp.BooleanExpression()))
		}
	} else if tt.LIKE() != nil {
		res.Type = PredicateTypeLike
		res.FirstValueExpression = NewValueExpressionNode(runtime, tt.GetPattern())
		if tt.ESCAPE() != nil {
			res.SecondValueExpression = NewValueExpressionNode(runtime, tt.GetEscape())
		}
	} else if tt.NULL() != nil {
		res.Type = PredicateTypeIsNull
	}
	return res
}

func (n *PredicateNode) GetType(_ *metadata.Metadata) (datatype.Type, error) {
	return datatype.BOOL, nil
}

func (n *PredicateNode) ExtractAggFunc(res *[]*FuncCallNode) {
	if n.FirstValueExpression != nil {
		n.FirstValueExpression.ExtractAggFunc(res)
	}

	if n.SecondValueExpression != nil {
		n.SecondValueExpression.ExtractAggFunc(res)
	}

	if n.BooleanExpressionNodes != nil {
		for _, booleanExpressionNode := range n.BooleanExpressionNodes {
			booleanExpressionNode.ExtractAggFunc(res)
		}
	}
}

func (n *PredicateNode) GetColumns() ([]string, error) {
	var s []string
	if n.FirstValueExpression != nil {
		if c, err := n.FirstValueExpression.GetColumns(); err != nil {
			return nil, err
		} else {
			s = append(s, c...)
		}
	}

	if n.SecondValueExpression != nil {
		if c, err := n.SecondValueExpression.GetColumns(); err != nil {
			return nil, err
		} else {
			s = append(s, c...)
		}
	}

	if n.BooleanExpressionNodes != nil {
		for _, booleanExpressionNode := range n.BooleanExpressionNodes {
			c, err := booleanExpressionNode.GetColumns()
			if err != nil {
				return nil, err
			}
			s = append(s, c...)
		}
	}

	return s, nil
}

func (n *PredicateNode) Init(md *metadata.Metadata) error {
	if n.FirstValueExpression != nil {
		if err := n.FirstValueExpression.Init(md); err != nil {
			return err
		}
	}

	if n.SecondValueExpression != nil {
		if err := n.SecondValueExpression.Init(md); err != nil {
			return err
		}
	}

	if n.BooleanExpressionNodes != nil {
		for _, booleanExpressionNode := range n.BooleanExpressionNodes {
			if err := booleanExpressionNode.Init(md); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *PredicateNode) Result(valsi interface{}, input *row.RowsGroup) (interface{}, error) {
	switch n.Type {
	case PredicateTypeComparison:
		fallthrough
	case PredicateTypeDistinctFrom:
		resi, err := n.FirstValueExpression.Result(input)
		if err != nil {
			return nil, err
		}
		vals, res := valsi.([]interface{}), resi.([]interface{})
		for i := 0; i < len(res); i++ {
			res[i] = datatype.OperatorFunc(vals[i], res[i], *n.ComparisonOperator)
		}
		return res, nil

	case PredicateTypeBetween:
		loweri, err := n.FirstValueExpression.Result(input)
		if err != nil {
			return nil, err
		}

		upperi, err := n.SecondValueExpression.Result(input)
		if err != nil {
			return nil, err
		}

		vals, lower, upper := valsi.([]interface{}), loweri.([]interface{}), upperi.([]interface{})

		res := make([]interface{}, len(vals))

		for i := 0; i < len(res); i++ {
			res[i] = datatype.OperatorFunc(vals[i], lower[i], datatype.GTE).(bool) && datatype.OperatorFunc(vals[i], upper[i], datatype.LTE).(bool)
			if n.IsNot {
				res[i] = !res[i].(bool)
			}
		}
		return res, nil

	case PredicateTypeIn:
		vals := valsi.([]interface{})
		res := make([]interface{}, len(vals))
		inItems := make([]interface{}, len(n.BooleanExpressionNodes))
		for i, booleanExpressionNode := range n.BooleanExpressionNodes {
			inItem, err := booleanExpressionNode.Result(input)
			if err != nil {
				return nil, err
			}
			inItems[i] = inItem
		}

		for i := 0; i < len(res); i++ {
			for _, item := range inItems {
				res[i] = datatype.EQFunc(vals[i], item.([]interface{})[i])
				if res[i].(bool) {
					break
				}
			}

			if n.IsNot {
				res[i] = !res[i].(bool)
			}
		}

		return res, nil

	case PredicateTypeLike:
		like := ""
		escape := ""
		iPattern, err := n.FirstValueExpression.Result(input)
		if err != nil {
			return nil, err
		}
		like = iPattern.([]interface{})[0].(string)

		if n.SecondValueExpression != nil {
			iEscape, err := n.SecondValueExpression.Result(input)
			if err != nil {
				return nil, err
			}
			escape = iEscape.([]string)[0]
		}

		matcher, err := likematcher.Compile(like, escape)
		if err != nil {
			return nil, err
		}
		vals := valsi.([]interface{})
		res := make([]interface{}, len(vals))
		for i := 0; i < len(res); i++ {
			res[i] = matcher.Match([]byte(datatype.ToString(vals[i])))
			if n.IsNot {
				res[i] = !res[i].(bool)
			}
		}
		return res, nil

	case PredicateTypeIsNull:
		vals := valsi.([]interface{})
		res := make([]interface{}, len(vals))
		for i := 0; i < len(res); i++ {
			if n.IsNot {
				res[i] = vals[i] != nil
			} else {
				res[i] = vals[i] == nil
			}
		}
		return res, nil

	default:
		return nil, fmt.Errorf("unknown predicate type")
	}

}
