package operator

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type CaseNode struct {
	Whens []*WhenClauseNode
	Else  *ExpressionNode
}

func NewCaseNode(runtime *config.Runtime, whens []parser.IWhenClauseContext, el parser.IExpressionContext) *CaseNode {
	res := &CaseNode{
		Whens: []*WhenClauseNode{},
		Else:  NewExpressionNode(runtime, el),
	}
	for _, w := range whens {
		res.Whens = append(res.Whens, NewWhenClauseNode(runtime, w))
	}
	return res
}

func (n *CaseNode) ExtractAggFunc(res *[]*FuncCallNode) {
	for _, w := range n.Whens {
		w.ExtractAggFunc(res)
	}
	n.Else.ExtractAggFunc(res)
}

func (n *CaseNode) GetColumns() ([]string, error) {
	var res []string
	resMap := map[string]int{}
	for _, w := range n.Whens {
		cs, err := w.GetColumns()
		if err != nil {
			return res, err
		}
		for _, c := range cs {
			resMap[c] = 1
		}
	}
	cs, err := n.Else.GetColumns()
	if err != nil {
		return res, err
	}
	for _, c := range cs {
		resMap[c] = 1
	}
	for c := range resMap {
		res = append(res, c)
	}
	return res, nil
}

func (n *CaseNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	for _, w := range n.Whens {
		return w.GetType(md)
	}
	return gtype.UNKNOWNTYPE, fmt.Errorf("unknown type")
}

func (n *CaseNode) Init(md *metadata.Metadata) error {
	for _, w := range n.Whens {
		if err := w.Init(md); err != nil {
			return err
		}
	}
	return nil
}

func (n *CaseNode) Result(input *row.RowsGroup) (interface{}, error) {
	var res interface{}
	var err error
	for _, w := range n.Whens {
		res, err = w.Result(input)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
	return n.Else.Result(input)
}

func (n *CaseNode) IsAggregate() bool {
	for _, w := range n.Whens {
		if w.IsAggregate() {
			return true
		}
	}
	if n.Else != nil && n.Else.IsAggregate() {
		return true
	}
	return false
}

type WhenClauseNode struct {
	Condition *ExpressionNode
	Res       *ExpressionNode
}

func NewWhenClauseNode(runtime *config.Runtime, wh parser.IWhenClauseContext) *WhenClauseNode {
	tt := wh.(*parser.WhenClauseContext)
	ct, rt := tt.GetCondition(), tt.GetResult()
	res := &WhenClauseNode{
		Condition: NewExpressionNode(runtime, ct),
		Res:       NewExpressionNode(runtime, rt),
	}
	return res
}

func (n *WhenClauseNode) ExtractAggFunc(res *[]*FuncCallNode) {
	n.Condition.ExtractAggFunc(res)
	n.Res.ExtractAggFunc(res)
}

func (n *WhenClauseNode) GetColumns() ([]string, error) {
	res, resmp := []string{}, map[string]int{}
	cs, err := n.Condition.GetColumns()
	if err != nil {
		return res, err
	}
	for _, c := range cs {
		resmp[c] = 1
	}
	cs, err = n.Res.GetColumns()
	if err != nil {
		return res, err
	}
	for _, c := range cs {
		resmp[c] = 1
	}
	for c := range resmp {
		res = append(res, c)
	}
	return res, nil
}

func (n *WhenClauseNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	return n.Res.GetType(md)
}

func (n *WhenClauseNode) Init(md *metadata.Metadata) error {
	if err := n.Condition.Init(md); err != nil {
		return err
	}
	if err := n.Res.Init(md); err != nil {
		return err
	}
	return nil
}

func (n *WhenClauseNode) Result(input *row.RowsGroup) (interface{}, error) {
	var res, cd interface{}
	var err error

	cd, err = n.Condition.Result(input)
	if err != nil {
		return nil, err
	}
	if cd.(bool) {
		res, err = n.Res.Result(input)
	}
	return res, err
}

func (n *WhenClauseNode) IsAggregate() bool {
	if n.Condition.IsAggregate() || n.Res.IsAggregate() {
		return true
	}
	return false
}
