package operator

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type PrimaryExpressionNode struct {
	//	Null         *NullNode
	Name string

	Number       *NumberNode
	BooleanValue *BooleanValueNode
	StringValue  *StringValueNode
	Identifier   *IdentifierNode

	//Func
	FuncCall *FuncCallNode
	//
	ParenthesizedExpression *ExpressionNode

	//t.name
	Base      *PrimaryExpressionNode
	FieldName *IdentifierNode
	Index     int //for performance

	//case when
	Case *CaseNode
}

func NewPrimaryExpressionNode(runtime *config.Runtime, t parser.IPrimaryExpressionContext) *PrimaryExpressionNode {
	tt := t.(*parser.PrimaryExpressionContext)
	res := &PrimaryExpressionNode{}
	children := tt.GetChildren()
	if tt.NULL() != nil {
		res.Name = "NULL"

	} else if tt.Identifier() != nil && tt.StringValue() != nil {
		res.Identifier = NewIdentifierNode(runtime, tt.Identifier())
		res.StringValue = NewStringValueNode(runtime, tt.StringValue())
		res.Name = "COL_" + res.Identifier.GetText()

	} else if nu := tt.Number(); nu != nil {
		res.Number = NewNumberNode(runtime, nu)
		res.Name = "COL_" + res.Number.Name

	} else if bv := tt.BooleanValue(); bv != nil {
		res.BooleanValue = NewBooleanValueNode(runtime, bv)
		res.Name = "COL_" + res.BooleanValue.Name

	} else if sv := tt.StringValue(); sv != nil {
		res.StringValue = NewStringValueNode(runtime, sv)
		res.Name = "COL_" + res.StringValue.Name

	} else if qn := tt.QualifiedName(); qn != nil {
		res.FuncCall = NewFuncCallNode(runtime, qn.GetText(), tt.SetQuantifier(), tt.AllExpression())
		res.Name = "COL_" + qn.GetText()

	} else if be := tt.GetBase(); be != nil {
		res.Base = NewPrimaryExpressionNode(runtime, be)
		res.FieldName = NewIdentifierNode(runtime, tt.GetFieldName())
		res.Name = res.Base.Name + "." + res.FieldName.GetText()

	} else if id := tt.Identifier(); id != nil {
		res.Identifier = NewIdentifierNode(runtime, id)
		res.Name = res.Identifier.GetText()

	} else if tt.CASE() != nil {
		res.Case = NewCaseNode(runtime, tt.AllWhenClause(), tt.GetElseExpression())
		res.Name = "CASE"

	} else {
		res.ParenthesizedExpression = NewExpressionNode(runtime, children[1].(parser.IExpressionContext))
		res.Name = res.ParenthesizedExpression.Name
	}

	return res
}

func (n *PrimaryExpressionNode) ExtractAggFunc(res *[]*FuncCallNode) {
	if n.FuncCall != nil && n.FuncCall.IsAggregate() {
		colName := fmt.Sprintf("AGG_%v_%v", len(*res), rand.Int())
		globalFunc := n.FuncCall
		globalFunc.ResColName = colName
		*res = append(*res, globalFunc)

		n.FuncCall = nil
		n.Identifier = &IdentifierNode{
			Str: &colName,
		}

	} else if n.Case != nil {
		n.Case.ExtractAggFunc(res)

	} else if n.ParenthesizedExpression != nil {
		n.ParenthesizedExpression.ExtractAggFunc(res)
	}

}

func (n *PrimaryExpressionNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	if n.Number != nil {
		return n.Number.GetType(md)

	} else if n.Identifier != nil && n.StringValue != nil {
		if n.Identifier.NonReserved == nil {
			return gtype.UNKNOWNTYPE, fmt.Errorf("GetType: wrong PrimaryExpressionNode")
		}
		t := strings.ToUpper(*n.Identifier.NonReserved)
		switch t {
		case "TIMESTAMP":
			return gtype.TIMESTAMP, nil
		}

	} else if n.BooleanValue != nil {
		return n.BooleanValue.GetType(md)

	} else if n.StringValue != nil {
		return n.StringValue.GetType(md)

	} else if n.Identifier != nil {
		return n.Identifier.GetType(md)

	} else if n.ParenthesizedExpression != nil {
		return n.ParenthesizedExpression.GetType(md)

	} else if n.FuncCall != nil {
		return n.FuncCall.GetType(md)

	} else if n.Case != nil {
		return n.Case.GetType(md)

	} else if n.Base != nil {
		return md.GetTypeByName(n.Name)
	}
	return gtype.UNKNOWNTYPE, fmt.Errorf("GetType: wrong PrimaryExpressionNode")
}

func (n *PrimaryExpressionNode) GetColumns() ([]string, error) {
	var res []string
	if n.Number != nil {
		return res, nil

	} else if n.Identifier != nil && n.StringValue != nil {
		return res, nil

	} else if n.BooleanValue != nil {
		return res, nil

	} else if n.StringValue != nil {
		return res, nil

	} else if n.Identifier != nil {
		return n.Identifier.GetColumns()

	} else if n.ParenthesizedExpression != nil {
		return n.ParenthesizedExpression.GetColumns()

	} else if n.FuncCall != nil {
		return n.FuncCall.GetColumns()

	} else if n.Case != nil {
		return n.Case.GetColumns()

	} else if n.Base != nil {
		return []string{n.Name}, nil
	}
	return res, fmt.Errorf("GetColumns: wrong PrimaryExpressionNode")
}

func (n *PrimaryExpressionNode) Init(md *metadata.Metadata) error {
	if n.Number != nil {
		return n.Number.Init(md)

	} else if n.BooleanValue != nil {
		return n.BooleanValue.Init(md)

	} else if n.StringValue != nil {
		return n.StringValue.Init(md)

	} else if n.Identifier != nil {
		return n.Identifier.Init(md)

	} else if n.ParenthesizedExpression != nil {
		return n.ParenthesizedExpression.Init(md)

	} else if n.FuncCall != nil {
		return n.FuncCall.Init(md)

	} else if n.Case != nil {
		return n.Case.Init(md)

	} else if n.Base != nil {
		index, err := md.GetIndexByName(n.Name)
		if err != nil {
			return err
		}
		n.Index = index
		return nil
	}
	return fmt.Errorf("result: wrong PrimaryExpressionNode")
}

func (n *PrimaryExpressionNode) Result(input *row.RowsGroup) (interface{}, error) {
	if n.Number != nil {
		return n.Number.Result(input)

	} else if n.Identifier != nil && n.StringValue != nil {
		t := *n.Identifier.NonReserved
		switch t {
		case "TIMESTAMP":
			resi, err := n.StringValue.Result(input)
			if err != nil {
				return nil, err
			}
			res := resi.([]interface{})
			for i := 0; i < len(res); i++ {
				res[i] = gtype.ToTimestamp(res[i])
			}
			return res, nil
		}

	} else if n.BooleanValue != nil {
		return n.BooleanValue.Result(input)

	} else if n.StringValue != nil {
		return n.StringValue.Result(input)

	} else if n.Identifier != nil {
		return n.Identifier.Result(input)

	} else if n.ParenthesizedExpression != nil {
		return n.ParenthesizedExpression.Result(input)

	} else if n.FuncCall != nil {
		return n.FuncCall.Result(input)

	} else if n.Case != nil {
		return n.Case.Result(input)

	} else if n.Base != nil {
		rn := input.GetRowsNumber()
		res := make([]interface{}, rn)
		index := n.Index
		for i := 0; i < rn; i++ {
			res[i] = input.Vals[index][i]
		}
		return res, nil
	}
	return nil, fmt.Errorf("result: wrong PrimaryExpressionNode")
}

func (n *PrimaryExpressionNode) IsAggregate() bool {
	if n.Number != nil {
		return false

	} else if n.Identifier != nil && n.StringValue != nil {
		return false

	} else if n.BooleanValue != nil {
		return false

	} else if n.StringValue != nil {
		return false

	} else if n.Identifier != nil {
		return false

	} else if n.ParenthesizedExpression != nil {
		return n.ParenthesizedExpression.IsAggregate()

	} else if n.Case != nil {
		return n.Case.IsAggregate()

	} else if n.FuncCall != nil {
		return n.FuncCall.IsAggregate()
	}
	return false
}
