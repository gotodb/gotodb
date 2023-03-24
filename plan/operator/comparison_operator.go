package operator

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/parser"
)

func NewComparisonOperator(runtime *config.Runtime, t parser.IComparisonOperatorContext) *gtype.Operator {
	tt := t.(*parser.ComparisonOperatorContext)
	var op gtype.Operator
	if tt.EQ() != nil {
		op = gtype.EQ
	} else if tt.NEQ() != nil {
		op = gtype.NEQ
	} else if tt.LT() != nil {
		op = gtype.LT
	} else if tt.LTE() != nil {
		op = gtype.LTE
	} else if tt.GT() != nil {
		op = gtype.GT
	} else if tt.GTE() != nil {
		op = gtype.GTE
	}
	return &op
}
