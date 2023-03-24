package operator

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type ValueExpressionNode struct {
	Name                  string
	PrimaryExpression     *PrimaryExpressionNode
	Operator              *gtype.Operator
	ValueExpression       *ValueExpressionNode
	BinaryValueExpression *BinaryValueExpressionNode
}

func NewValueExpressionNode(runtime *config.Runtime, t parser.IValueExpressionContext) *ValueExpressionNode {
	tt := t.(*parser.ValueExpressionContext)
	res := &ValueExpressionNode{}
	children := t.GetChildren()
	switch len(children) {
	case 1: //PrimaryExpression
		res.PrimaryExpression = NewPrimaryExpressionNode(runtime, tt.PrimaryExpression())
		res.Name = res.PrimaryExpression.Name

	case 2: //ValueExpression
		ops := "+"
		if tt.MINUS() != nil {
			res.Operator = gtype.NewOperatorFromString("-")
			ops = "-"
		} else {
			res.Operator = gtype.NewOperatorFromString("+")
			ops = "+"
		}
		res.ValueExpression = NewValueExpressionNode(runtime, children[1].(parser.IValueExpressionContext))
		res.Name = ops + res.ValueExpression.Name

	case 3: //BinaryValueExpression
		op := gtype.NewOperatorFromString(children[1].(*antlr.TerminalNodeImpl).GetText())
		res.BinaryValueExpression = NewBinaryValueExpressionNode(runtime, tt.GetLeft(), tt.GetRight(), op)
		res.Name = res.BinaryValueExpression.Name
	}
	return res
}

func (n *ValueExpressionNode) ExtractAggFunc(res *[]*FuncCallNode) {
	if n.PrimaryExpression != nil {
		n.PrimaryExpression.ExtractAggFunc(res)
	} else if n.ValueExpression != nil {
		n.ValueExpression.ExtractAggFunc(res)
	} else if n.BinaryValueExpression != nil {
		n.BinaryValueExpression.ExtractAggFunc(res)
	}
}

func (n *ValueExpressionNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	if n.PrimaryExpression != nil {
		return n.PrimaryExpression.GetType(md)
	} else if n.ValueExpression != nil {
		return n.ValueExpression.GetType(md)
	} else if n.BinaryValueExpression != nil {
		return n.BinaryValueExpression.GetType(md)
	}
	return gtype.UNKNOWNTYPE, fmt.Errorf("ValueExpressionNode type error")
}

func (n *ValueExpressionNode) GetColumns() ([]string, error) {
	if n.PrimaryExpression != nil {
		return n.PrimaryExpression.GetColumns()
	} else if n.ValueExpression != nil {
		return n.PrimaryExpression.GetColumns()
	} else if n.BinaryValueExpression != nil {
		return n.BinaryValueExpression.GetColumns()
	}
	return []string{}, fmt.Errorf("ValueExpression node error")
}

func (n *ValueExpressionNode) Init(md *metadata.Metadata) error {
	if n.PrimaryExpression != nil {
		return n.PrimaryExpression.Init(md)

	} else if n.ValueExpression != nil {
		return n.ValueExpression.Init(md)

	} else if n.BinaryValueExpression != nil {
		return n.BinaryValueExpression.Init(md)
	}
	return fmt.Errorf("wrong ValueExpressionNode")
}

func (n *ValueExpressionNode) Result(input *row.RowsGroup) (interface{}, error) {
	if n.PrimaryExpression != nil {
		return n.PrimaryExpression.Result(input)

	} else if n.ValueExpression != nil {
		if *n.Operator == gtype.MINUS {
			resi, err := n.ValueExpression.Result(input)
			if err != nil {
				return nil, err
			}
			res := resi.([]interface{})
			for i := 0; i < len(res); i++ {
				res[i] = gtype.OperatorFunc(-1, res[i], gtype.ASTERISK)
			}
			return res, nil
		}
		return n.ValueExpression.Result(input)

	} else if n.BinaryValueExpression != nil {
		return n.BinaryValueExpression.Result(input)
	}
	return nil, fmt.Errorf("wrong ValueExpressionNode")
}

func (n *ValueExpressionNode) IsAggregate() bool {
	if n.PrimaryExpression != nil {
		return n.PrimaryExpression.IsAggregate()

	} else if n.ValueExpression != nil {
		return n.ValueExpression.IsAggregate()

	} else if n.BinaryValueExpression != nil {
		return n.BinaryValueExpression.IsAggregate()
	}
	return false
}

type BinaryValueExpressionNode struct {
	Name                 string
	LeftValueExpression  *ValueExpressionNode
	RightValueExpression *ValueExpressionNode
	Operator             *gtype.Operator
}

func NewBinaryValueExpressionNode(
	runtime *config.Runtime,
	left parser.IValueExpressionContext,
	right parser.IValueExpressionContext,
	op *gtype.Operator) *BinaryValueExpressionNode {

	res := &BinaryValueExpressionNode{
		LeftValueExpression:  NewValueExpressionNode(runtime, left),
		RightValueExpression: NewValueExpressionNode(runtime, right),
		Operator:             op,
	}
	res.Name = res.LeftValueExpression.Name + "_" + res.RightValueExpression.Name
	return res
}

func (n *BinaryValueExpressionNode) ExtractAggFunc(res *[]*FuncCallNode) {
	n.LeftValueExpression.ExtractAggFunc(res)
	n.RightValueExpression.ExtractAggFunc(res)
}

func (n *BinaryValueExpressionNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	lt, errL := n.LeftValueExpression.GetType(md)
	if errL != nil {
		return lt, errL
	}
	rt, errR := n.RightValueExpression.GetType(md)
	if errR != nil {
		return rt, errR
	}
	return gtype.CheckType(lt, rt, *n.Operator)
}

func (n *BinaryValueExpressionNode) GetColumns() ([]string, error) {
	var res []string
	resmp := map[string]int{}
	rl, err := n.LeftValueExpression.GetColumns()
	if err != nil {
		return res, err
	}
	rr, err := n.RightValueExpression.GetColumns()
	if err != nil {
		return res, err
	}
	for _, c := range rl {
		resmp[c] = 1
	}
	for _, c := range rr {
		resmp[c] = 1
	}
	for c := range resmp {
		res = append(res, c)
	}
	return res, nil
}

func (n *BinaryValueExpressionNode) Init(md *metadata.Metadata) error {
	if err := n.LeftValueExpression.Init(md); err != nil {
		return err
	}
	if err := n.RightValueExpression.Init(md); err != nil {
		return err
	}
	return nil
}

func (n *BinaryValueExpressionNode) Result(input *row.RowsGroup) (interface{}, error) {
	leftValsi, errL := n.LeftValueExpression.Result(input)
	if errL != nil {
		return nil, errL
	}
	rightValsi, errR := n.RightValueExpression.Result(input)
	if errR != nil {
		return nil, errR
	}
	leftVals, rightVals := leftValsi.([]interface{}), rightValsi.([]interface{})
	if len(leftVals) != len(rightVals) {
		return nil, fmt.Errorf("BinaryValueExpressionNode: length not math")
	}
	res := make([]interface{}, len(leftVals))
	for i := 0; i < len(leftVals); i++ {
		res[i] = gtype.OperatorFunc(leftVals[i], rightVals[i], *n.Operator)
	}
	return res, nil
}

func (n *BinaryValueExpressionNode) IsAggregate() bool {
	return n.LeftValueExpression.IsAggregate() || n.RightValueExpression.IsAggregate()
}
