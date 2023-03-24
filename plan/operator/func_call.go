package operator

import (
	"fmt"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type IFunc struct {
	Name        string
	Result      func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error)
	IsAggregate func(es []*ExpressionNode) bool
	GetType     func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error)
	Init        func()
}

var Funcs map[string]func() *IFunc

func init() {
	Funcs = map[string]func() *IFunc{
		//aggregate functions
		"SUM":         NewSumFunc,
		"SUMGLOBAL":   NewSumGlobalFunc,
		"AVG":         NewAvgFunc,
		"AVGGLOBAL":   NewAvgGlobalFunc,
		"MAX":         NewMaxFunc,
		"MAXGLOBAL":   NewMaxGlobalFunc,
		"MIN":         NewMinFunc,
		"MINGLOBAL":   NewMinGlobalFunc,
		"COUNT":       NewCountFunc,
		"COUNTGLOBAL": NewCountGlobalFunc,

		//math functions
		"ABS":    NewAbsFunc,
		"SQRT":   NewSqrtFunc,
		"POW":    NewPowFunc,
		"RAND":   NewRandomFunc,
		"RANDOM": NewRandomFunc,

		"LOG":   NewLogFunc,
		"LOG10": NewLog10Func,
		"LOG2":  NewLog2Func,
		"LN":    NewLnFunc,

		"FLOOR":   NewFloorFunc,
		"CEIL":    NewCeilFunc,
		"CEILING": NewCeilFunc,
		"ROUND":   NewRoundFunc,

		"SIN":  NewSinFunc,
		"COS":  NewCosFunc,
		"TAN":  NewTanFunc,
		"ASIN": NewASinFunc,
		"ACOS": NewACosFunc,
		"ATAN": NewATanFunc,

		"SINH":  NewSinhFunc,
		"COSH":  NewCoshFunc,
		"TANH":  NewTanhFunc,
		"ASINH": NewASinhFunc,
		"ACOSH": NewACoshFunc,
		"ATANH": NewATanhFunc,

		"E":  NewEFunc,
		"PI": NewPiFunc,

		//string functions
		"LENGTH":  NewLengthFunc,
		"LOWER":   NewLowerFunc,
		"UPPER":   NewUpperFunc,
		"CONCAT":  NewConcatFunc,
		"REVERSE": NewReverseFunc,
		"SUBSTR":  NewSubstrFunc,
		"REPLACE": NewReplaceFunc,

		//time functions
		"NOW":    NewNowFunc,
		"DAY":    NewDayFunc,
		"MONTH":  NewMonthFunc,
		"YEAR":   NewYearFunc,
		"HOUR":   NewHourFunc,
		"MINUTE": NewMinuteFunc,
		"SECOND": NewSecondFunc,
	}
}

////////////////////////

type FuncCallNode struct {
	FuncName      string
	ResColName    string //used in ExtractAggFunc
	Func          *IFunc
	SetQuantifier *gtype.QuantifierType
	Expressions   []*ExpressionNode
}

func NewFuncCallNode(runtime *config.Runtime, name string, sq parser.ISetQuantifierContext, expressions []parser.IExpressionContext) *FuncCallNode {
	name = strings.ToUpper(name)
	res := &FuncCallNode{
		FuncName:      name,
		SetQuantifier: nil,
		Expressions:   make([]*ExpressionNode, len(expressions)),
	}
	for i := 0; i < len(expressions); i++ {
		res.Expressions[i] = NewExpressionNode(runtime, expressions[i])
	}

	if sq != nil {
		q := gtype.StrToQuantifierType(sq.GetText())
		res.SetQuantifier = &q
	}
	return res
}

func (n *FuncCallNode) Init(md *metadata.Metadata) error {
	for _, e := range n.Expressions {
		if err := e.Init(md); err != nil {
			return err
		}
	}

	if n.Func == nil {
		if f, ok := Funcs[n.FuncName]; ok {
			n.Func = f()
		} else {
			return fmt.Errorf("unknown function %v", n.FuncName)
		}
	}
	n.Func.Init()
	return nil
}

func (n *FuncCallNode) Result(input *row.RowsGroup) (interface{}, error) {
	if n.Func != nil {
		return n.Func.Result(input, n.SetQuantifier, n.Expressions)
	}
	return nil, fmt.Errorf("unkown function %v", n.FuncName)
}

func (n *FuncCallNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	if fun, ok := Funcs[n.FuncName]; ok {
		return fun().GetType(md, n.Expressions)
	}
	return gtype.UNKNOWNTYPE, fmt.Errorf("unkown function %v", n.FuncName)
}

func (n *FuncCallNode) GetColumns() ([]string, error) {
	var res []string
	resmp := map[string]int{}
	for _, e := range n.Expressions {
		cs, err := e.GetColumns()
		if err != nil {
			return res, err
		}
		for _, c := range cs {
			resmp[c] = 1
		}
	}
	for c, _ := range resmp {
		res = append(res, c)
	}
	return res, nil
}

func (n *FuncCallNode) IsAggregate() bool {
	if fun, ok := Funcs[n.FuncName]; ok {
		return fun().IsAggregate(n.Expressions)
	}
	return false
}
