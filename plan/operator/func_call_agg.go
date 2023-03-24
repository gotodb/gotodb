package operator

import (
	"fmt"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

func AggLocalFuncToAggGlobalFunc(f *IFunc) *IFunc {
	switch f.Name {
	case "COUNT":
		return NewCountGlobalFunc()
	case "SUM":
		return NewSumGlobalFunc()
	case "AVG":
		return NewAvgGlobalFunc()
	case "MIN":
		return NewMinGlobalFunc()
	case "MAX":
		return NewMaxGlobalFunc()
	}
	return nil
}

func NewCountGlobalFunc() *IFunc {
	var funcRes map[string]interface{}

	res := &IFunc{
		Name: "COUNTGLOBAL",
		IsAggregate: func(es []*ExpressionNode) bool {
			return true
		},

		Init: func() {
			funcRes = make(map[string]interface{})
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error) {
			return gtype.INT64, nil
		},

		Result: func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in SUM")
			}
			var (
				err error
				esi interface{}
				t   = Expressions[0]
			)

			if esi, err = t.Result(input); err != nil {
				return nil, err
			}
			es := esi.([]interface{})

			for i := 0; i < len(es); i++ {
				key := input.GetKeyString(i)
				if _, ok := funcRes[key]; !ok || funcRes[key] == nil {
					funcRes[key] = es[i]
				} else if es[i] != nil {
					funcRes[key] = gtype.OperatorFunc(funcRes[key], es[i], gtype.PLUS)
				}
			}
			return funcRes, err
		},
	}
	return res
}

func NewCountFunc() *IFunc {
	var funcRes map[string]interface{}

	res := &IFunc{
		Name: "COUNT",
		IsAggregate: func(es []*ExpressionNode) bool {
			return true
		},

		Init: func() {
			funcRes = make(map[string]interface{})
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error) {
			return gtype.INT64, nil
		},

		Result: func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in SUM")
			}
			var (
				err error
				esi interface{}
				t   = Expressions[0]
			)

			if esi, err = t.Result(input); err != nil {
				return nil, err
			}
			es := esi.([]interface{})
			for i := 0; i < len(es); i++ {
				if es[i] == nil {
					continue
				}
				key := input.GetKeyString(i)

				if _, ok := funcRes[key]; !ok || funcRes[key] == nil {
					funcRes[key] = int64(1)
				} else if es[i] != nil {
					funcRes[key] = funcRes[key].(int64) + int64(1)
				}
			}
			return funcRes, err
		},
	}
	return res
}

func NewSumGlobalFunc() *IFunc {
	res := NewSumFunc()
	res.Name = "SUMGLOBAL"
	return res

}

func NewSumFunc() *IFunc {
	var funcRes map[string]interface{}

	res := &IFunc{
		Name: "SUM",
		IsAggregate: func(es []*ExpressionNode) bool {
			return true
		},

		Init: func() {
			funcRes = make(map[string]interface{})
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error) {
			if len(es) < 1 {
				return gtype.UNKNOWNTYPE, fmt.Errorf("not enough parameters in SUM")
			}
			return es[0].GetType(md)
		},

		Result: func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in SUM")
			}
			var (
				err error
				esi interface{}
				t   = Expressions[0]
			)

			if esi, err = t.Result(input); err != nil {
				return nil, err
			}
			es := esi.([]interface{})

			for i := 0; i < len(es); i++ {
				if es[i] == nil {
					continue
				}
				key := input.GetKeyString(i)

				if _, ok := funcRes[key]; !ok || funcRes[key] == nil {
					funcRes[key] = es[i]
				} else if es[i] != nil {
					funcRes[key] = gtype.OperatorFunc(funcRes[key], es[i], gtype.PLUS)
				}
			}
			return funcRes, err
		},
	}
	return res
}

func NewAvgGlobalFunc() *IFunc {
	var funcRes map[string]interface{}

	res := &IFunc{
		Name: "AVGGLOBAL",
		IsAggregate: func(es []*ExpressionNode) bool {
			return true
		},

		Init: func() {
			funcRes = make(map[string]interface{})
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error) {
			return gtype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in AVG")
			}
			var (
				err error
				esi interface{}
				t   = Expressions[0]
			)

			if esi, err = t.Result(input); err != nil {
				return nil, err
			}
			es := esi.([]interface{})

			for i := 0; i < len(es); i++ {
				key := input.GetKeyString(i)

				if _, ok := funcRes[key]; !ok || funcRes[key] == nil {
					funcRes[key] = fmt.Sprintf("%v:%v", es[i], 1)
				} else if es[i] != nil {
					var sumctmp, cntctmp float64
					fmt.Sscanf(es[i].(string), "%f:%f", &sumctmp, &cntctmp)
					var sumc, cntc float64
					fmt.Sscanf(funcRes[key].(string), "%f:%f", &sumc, &cntc)
					funcRes[key] = fmt.Sprintf("%v:%v", sumc+sumctmp, cntc+cntctmp)
				}
			}
			res := make(map[string]interface{})
			for k, v := range res {
				var sum, cnt float64
				fmt.Sscanf(v.(string), "%f:%f", &sum, &cnt)
				res[k] = sum / cnt

			}
			return res, err
		},
	}
	return res
}

func NewAvgFunc() *IFunc {
	var funcRes map[string]interface{}

	res := &IFunc{
		Name: "AVG",
		IsAggregate: func(es []*ExpressionNode) bool {
			return true
		},

		Init: func() {
			funcRes = make(map[string]interface{})
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error) {
			return gtype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in AVG")
			}
			var (
				err error
				esi interface{}
				t   = Expressions[0]
			)

			if esi, err = t.Result(input); err != nil {
				return nil, err
			}
			es := esi.([]interface{})

			for i := 0; i < len(es); i++ {
				if es[i] == nil {
					continue
				}
				key := input.GetKeyString(i)

				if _, ok := funcRes[key]; !ok || funcRes[key] == nil {
					funcRes[key] = fmt.Sprintf("%v:%v", es[i], 1)
				} else if es[i] != nil {
					var sumc, cntc float64
					fmt.Sscanf(funcRes[key].(string), "%f:%f", &sumc, &cntc)
					sumc = sumc + gtype.ToFloat64(es[i])
					cntc = cntc + float64(1)
					funcRes[key] = fmt.Sprintf("%v:%v", sumc, cntc)
				}
			}
			return funcRes, err
		},
	}
	return res
}

func NewMinGlobalFunc() *IFunc {
	res := NewMinFunc()
	res.Name = "MINGLOBAL"
	return res
}

func NewMinFunc() *IFunc {
	var funcRes map[string]interface{}

	res := &IFunc{
		Name: "MIN",
		IsAggregate: func(es []*ExpressionNode) bool {
			return true
		},

		Init: func() {
			funcRes = make(map[string]interface{})
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error) {
			if len(es) < 1 {
				return gtype.UNKNOWNTYPE, fmt.Errorf("not enough parameters in MIN")
			}
			return es[0].GetType(md)
		},

		Result: func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in MIN")
			}
			var (
				err error
				esi interface{}
				t   = Expressions[0]
			)

			if esi, err = t.Result(input); err != nil {
				return nil, err
			}
			es := esi.([]interface{})

			for i := 0; i < len(es); i++ {
				if es[i] == nil {
					continue
				}
				key := input.GetKeyString(i)

				if _, ok := funcRes[key]; !ok || funcRes[key] == nil {
					funcRes[key] = es[i]
				} else if es[i] != nil {
					if gtype.GTFunc(funcRes[key], es[i]).(bool) {
						funcRes[key] = es[i]
					}
				}
			}
			return funcRes, err
		},
	}
	return res
}

func NewMaxGlobalFunc() *IFunc {
	res := NewMaxFunc()
	res.Name = "MAXGLOBAL"
	return res
}

func NewMaxFunc() *IFunc {
	var funcRes map[string]interface{}

	res := &IFunc{
		Name: "MAX",
		IsAggregate: func(ex []*ExpressionNode) bool {
			return true
		},

		Init: func() {
			funcRes = make(map[string]interface{})
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (gtype.Type, error) {
			if len(es) < 1 {
				return gtype.UNKNOWNTYPE, fmt.Errorf("not enough parameters in MAX")
			}
			return es[0].GetType(md)
		},

		Result: func(input *row.RowsGroup, sq *gtype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in MAX")
			}
			var (
				err error
				esi interface{}
				t   = Expressions[0]
			)
			if esi, err = t.Result(input); err != nil {
				return nil, err
			}
			es := esi.([]interface{})

			for i := 0; i < len(es); i++ {
				if es[i] == nil {
					continue
				}
				key := input.GetKeyString(i)

				if _, ok := funcRes[key]; !ok || funcRes[key] == nil {
					funcRes[key] = es[i]
				} else if es[i] != nil {
					if gtype.LTFunc(funcRes[key], es[i]).(bool) {
						funcRes[key] = es[i]
					}
				}
			}
			return funcRes, err
		},
	}
	return res
}
