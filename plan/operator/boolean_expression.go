package operator

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type BooleanExpressionNode struct {
	Name                    string
	Clause                  string
	Predicated              *PredicatedNode
	NotBooleanExpression    *NotBooleanExpressionNode
	BinaryBooleanExpression *BinaryBooleanExpressionNode
}

func NewBooleanExpressionNode(runtime *config.Runtime, t parser.IBooleanExpressionContext) *BooleanExpressionNode {
	tt := t.(*parser.BooleanExpressionContext)

	res := &BooleanExpressionNode{}
	children := tt.GetChildren()
	switch len(children) {
	case 1: //Predicated
		res.Predicated = NewPredicatedNode(runtime, tt.Predicated())
		res.Name = res.Predicated.Name

	case 2: //NOT
		res.NotBooleanExpression = NewNotBooleanExpressionNode(runtime, tt.BooleanExpression(0))
		res.Name = res.NotBooleanExpression.Name

	case 3: //Binary
		var o gtype.Operator
		if tt.AND() != nil {
			o = gtype.AND
		} else if tt.OR() != nil {
			o = gtype.OR
		}
		res.BinaryBooleanExpression = NewBinaryBooleanExpressionNode(runtime, tt.GetLeft(), tt.GetRight(), o)
		res.Name = res.BinaryBooleanExpression.Name

	}

	start := tt.GetStart().GetStart()
	stop := tt.GetStop().GetStop()
	res.Clause = tt.GetStart().GetTokenSource().GetInputStream().GetText(start, stop)

	return res
}

func (n *BooleanExpressionNode) ExtractAggFunc(res *[]*FuncCallNode) {
	if n.Predicated != nil {
		n.Predicated.ExtractAggFunc(res)
	} else if n.NotBooleanExpression != nil {
		n.NotBooleanExpression.ExtractAggFunc(res)
	} else if n.BinaryBooleanExpression != nil {
		n.BinaryBooleanExpression.ExtractAggFunc(res)
	}
}

func (n *BooleanExpressionNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	if n.Predicated != nil {
		return n.Predicated.GetType(md)
	} else if n.NotBooleanExpression != nil {
		return n.NotBooleanExpression.GetType(md)
	} else if n.BinaryBooleanExpression != nil {
		return n.BinaryBooleanExpression.GetType(md)
	}
	return gtype.UNKNOWNTYPE, fmt.Errorf("GetType: wrong BooleanExpressionNode")
}

func (n *BooleanExpressionNode) GetColumns() ([]string, error) {
	if n.Predicated != nil {
		return n.Predicated.GetColumns()
	} else if n.NotBooleanExpression != nil {
		return n.NotBooleanExpression.GetColumns()
	} else if n.BinaryBooleanExpression != nil {
		return n.BinaryBooleanExpression.GetColumns()
	}
	return nil, fmt.Errorf("GetColumns: wrong BooleanExpressionNode")
}

func (n *BooleanExpressionNode) Init(md *metadata.Metadata) error {
	if n.Predicated != nil {
		return n.Predicated.Init(md)
	} else if n.NotBooleanExpression != nil {
		return n.NotBooleanExpression.Init(md)
	} else if n.BinaryBooleanExpression != nil {
		return n.BinaryBooleanExpression.Init(md)
	}
	return fmt.Errorf("wrong BooleanExpressionNode")
}

func (n *BooleanExpressionNode) Result(input *row.RowsGroup) (interface{}, error) {
	if n.Predicated != nil {
		return n.Predicated.Result(input)
	} else if n.NotBooleanExpression != nil {
		return n.NotBooleanExpression.Result(input)
	} else if n.BinaryBooleanExpression != nil {
		return n.BinaryBooleanExpression.Result(input)
	}
	return nil, fmt.Errorf("wrong BooleanExpressionNode")
}

func (n *BooleanExpressionNode) IsAggregate() bool {
	if n.Predicated != nil {
		return n.Predicated.IsAggregate()
	} else if n.NotBooleanExpression != nil {
		return n.NotBooleanExpression.IsAggregate()
	} else if n.BinaryBooleanExpression != nil {
		return n.BinaryBooleanExpression.IsAggregate()
	}
	return false
}

type NotBooleanExpressionNode struct {
	Name              string
	BooleanExpression *BooleanExpressionNode
}

func NewNotBooleanExpressionNode(runtime *config.Runtime, t parser.IBooleanExpressionContext) *NotBooleanExpressionNode {
	res := &NotBooleanExpressionNode{
		BooleanExpression: NewBooleanExpressionNode(runtime, t),
	}
	res.Name = "NOT_" + res.BooleanExpression.Name
	return res
}

func (n *NotBooleanExpressionNode) ExtractAggFunc(res *[]*FuncCallNode) {
	n.BooleanExpression.ExtractAggFunc(res)
}

func (n *NotBooleanExpressionNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	t, err := n.BooleanExpression.GetType(md)
	if err != nil {
		return t, err
	}
	if t != gtype.BOOL {
		return t, fmt.Errorf("expression type error")
	}
	return t, nil
}

func (n *NotBooleanExpressionNode) GetColumns() ([]string, error) {
	return n.BooleanExpression.GetColumns()
}

func (n *NotBooleanExpressionNode) Init(md *metadata.Metadata) error {
	return n.BooleanExpression.Init(md)
}

func (n *NotBooleanExpressionNode) Result(input *row.RowsGroup) (interface{}, error) {
	resi, err := n.BooleanExpression.Result(input)
	if err != nil {
		return false, err
	}

	res := resi.([]interface{})
	for i := 0; i < len(res); i++ {
		res[i] = !(res[i].(bool))
	}
	return res, nil
}

func (n *NotBooleanExpressionNode) IsAggregate() bool {
	return n.BooleanExpression.IsAggregate()
}

type BinaryBooleanExpressionNode struct {
	Name                   string
	LeftBooleanExpression  *BooleanExpressionNode
	RightBooleanExpression *BooleanExpressionNode
	Operator               *gtype.Operator
}

func NewBinaryBooleanExpressionNode(
	runtime *config.Runtime,
	left parser.IBooleanExpressionContext,
	right parser.IBooleanExpressionContext,
	op gtype.Operator) *BinaryBooleanExpressionNode {

	res := &BinaryBooleanExpressionNode{
		LeftBooleanExpression:  NewBooleanExpressionNode(runtime, left),
		RightBooleanExpression: NewBooleanExpressionNode(runtime, right),
		Operator:               &op,
	}
	res.Name = res.LeftBooleanExpression.Name + "_" + res.RightBooleanExpression.Name
	return res
}

func (n *BinaryBooleanExpressionNode) ExtractAggFunc(res *[]*FuncCallNode) {
	n.LeftBooleanExpression.ExtractAggFunc(res)
	n.RightBooleanExpression.ExtractAggFunc(res)
}

func (n *BinaryBooleanExpressionNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	lt, err1 := n.LeftBooleanExpression.GetType(md)
	if err1 != nil {
		return gtype.UNKNOWNTYPE, err1
	}
	if lt != gtype.BOOL {
		return lt, fmt.Errorf("expression type error")
	}
	rt, err2 := n.RightBooleanExpression.GetType(md)
	if err2 != nil {
		return gtype.UNKNOWNTYPE, err2
	}
	if rt != gtype.BOOL {
		return rt, fmt.Errorf("expression type error")
	}

	return gtype.BOOL, nil
}

func (n *BinaryBooleanExpressionNode) GetColumns() ([]string, error) {
	resmp := make(map[string]int)
	var res []string
	rl, err := n.LeftBooleanExpression.GetColumns()
	if err != nil {
		return res, err
	}
	rr, err := n.RightBooleanExpression.GetColumns()
	if err != nil {
		return res, err
	}
	for _, c := range rl {
		resmp[c] = 1
	}
	for _, c := range rr {
		resmp[c] = 1
	}
	for key := range resmp {
		res = append(res, key)
	}
	return res, nil
}

func (n *BinaryBooleanExpressionNode) Init(md *metadata.Metadata) error {
	if err := n.LeftBooleanExpression.Init(md); err != nil {
		return err
	}
	if err := n.RightBooleanExpression.Init(md); err != nil {
		return err
	}
	return nil
}

func (n *BinaryBooleanExpressionNode) Result(input *row.RowsGroup) (interface{}, error) {
	leftResi, err := n.LeftBooleanExpression.Result(input)
	if err != nil {
		return nil, err
	}
	rightResi, err := n.RightBooleanExpression.Result(input)
	if err != nil {
		return nil, err
	}

	leftRes, rightRes := leftResi.([]interface{}), rightResi.([]interface{})
	for i := 0; i < input.GetRowsNumber(); i++ {
		if *n.Operator == gtype.AND {
			leftRes[i] = leftRes[i].(bool) && rightRes[i].(bool)
		} else if *n.Operator == gtype.OR {
			leftRes[i] = leftRes[i].(bool) || rightRes[i].(bool)
		}
	}
	return leftRes, nil
}

func (n *BinaryBooleanExpressionNode) IsAggregate() bool {
	return n.LeftBooleanExpression.IsAggregate() || n.RightBooleanExpression.IsAggregate()
}
