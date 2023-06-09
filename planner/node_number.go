package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/pkg/parser"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type NumberNode struct {
	Name      string
	DoubleVal *float64
	IntVal    *int64
}

func NewNumberNode(_ *config.Runtime, t parser.INumberContext) *NumberNode {
	tt := t.(*parser.NumberContext)
	res := &NumberNode{}
	res.Name = tt.GetText()
	if dv := tt.DOUBLE_VALUE(); dv != nil {
		var v float64
		fmt.Sscanf(dv.GetText(), "%f", &v)
		res.DoubleVal = &v
	} else if iv := tt.INTEGER_VALUE(); iv != nil {
		var v int64
		fmt.Sscanf(iv.GetText(), "%d", &v)
		res.IntVal = &v
	}
	return res
}

func (n *NumberNode) Init(_ *metadata.Metadata) error {
	return nil
}

func (n *NumberNode) Result(input *row.RowsGroup) (interface{}, error) {
	rn := input.GetRowsNumber()
	res := make([]interface{}, rn)
	if n.DoubleVal != nil {
		for i := 0; i < rn; i++ {
			res[i] = *n.DoubleVal
		}
	} else if n.IntVal != nil {
		for i := 0; i < rn; i++ {
			res[i] = *n.IntVal
		}
	} else {
		return nil, fmt.Errorf("wrong NumberNode")
	}
	return res, nil
}

func (n *NumberNode) GetType(_ *metadata.Metadata) (datatype.Type, error) {
	if n.DoubleVal != nil {
		return datatype.FLOAT64, nil
	} else if n.IntVal != nil {
		return datatype.INT64, nil
	}
	return datatype.UnknownType, fmt.Errorf("wrong NumberNode")
}

func (n *NumberNode) GetText() string {
	if n.DoubleVal != nil {
		return fmt.Sprintf("%f", *n.DoubleVal)
	} else if n.IntVal != nil {
		return fmt.Sprintf("%d", *n.IntVal)
	} else {
		return ""
	}
}
