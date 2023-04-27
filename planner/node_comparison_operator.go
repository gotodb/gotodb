package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/pkg/parser"
)

func NewComparisonOperator(runtime *config.Runtime, t parser.IComparisonOperatorContext) *datatype.Operator {
	tt := t.(*parser.ComparisonOperatorContext)
	var op datatype.Operator
	if tt.EQ() != nil {
		op = datatype.EQ
	} else if tt.NEQ() != nil {
		op = datatype.NEQ
	} else if tt.LT() != nil {
		op = datatype.LT
	} else if tt.LTE() != nil {
		op = datatype.LTE
	} else if tt.GT() != nil {
		op = datatype.GT
	} else if tt.GTE() != nil {
		op = datatype.GTE
	}
	return &op
}
