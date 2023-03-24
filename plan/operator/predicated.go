package operator

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type PredicatedNode struct {
	Name            string
	ValueExpression *ValueExpressionNode
	Predicate       *PredicateNode
}

func NewPredicatedNode(runtime *config.Runtime, t parser.IPredicatedContext) *PredicatedNode {
	tt := t.(*parser.PredicatedContext)
	res := &PredicatedNode{}
	res.ValueExpression = NewValueExpressionNode(runtime, tt.ValueExpression())
	if tp := tt.Predicate(); tp != nil {
		res.Predicate = NewPredicateNode(runtime, tp)
	}
	res.Name = res.ValueExpression.Name
	return res
}

func (n *PredicatedNode) ExtractAggFunc(res *[]*FuncCallNode) {
	n.ValueExpression.ExtractAggFunc(res)
	if n.Predicate != nil {
		n.Predicate.ExtractAggFunc(res)
	}
}

func (n *PredicatedNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	t, err := n.ValueExpression.GetType(md)
	if err != nil {
		return t, err
	}
	if n.Predicate != nil {
		return n.Predicate.GetType(md)
	}
	return t, nil
}

func (n *PredicatedNode) GetColumns() ([]string, error) {
	var (
		err   error
		res   []string
		resmp = map[string]int{}
		rp    []string
		rv    []string
	)

	rv, err = n.ValueExpression.GetColumns()
	if err != nil {
		return res, err
	}
	if n.Predicate != nil {
		rp, err = n.Predicate.GetColumns()
		if err != nil {
			return res, err
		}
	}
	for _, c := range rv {
		resmp[c] = 1
	}
	for _, c := range rp {
		resmp[c] = 1
	}
	for c := range resmp {
		res = append(res, c)
	}
	return res, nil
}

func (n *PredicatedNode) Init(md *metadata.Metadata) error {
	if err := n.ValueExpression.Init(md); err != nil {
		return err
	}
	if n.Predicate != nil {
		return n.Predicate.Init(md)
	}
	return nil
}

func (n *PredicatedNode) Result(input *row.RowsGroup) (interface{}, error) {
	res, err := n.ValueExpression.Result(input)
	if err != nil {
		return nil, err
	}
	if n.Predicate == nil {
		return res, nil
	}
	return n.Predicate.Result(res, input)
}

func (n *PredicatedNode) IsAggregate() bool {
	return n.ValueExpression.IsAggregate()
}
