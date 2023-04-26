package operator

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

func NewAbsFunc() *IFunc {
	res := &IFunc{
		Name: "ABS",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			if len(es) < 1 {
				return datatype.UnknownType, fmt.Errorf("not enough parameters in Abs")
			}
			return es[0].GetType(md)
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in Abs")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return nil, err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return nil, fmt.Errorf("type cann't use ABS function")
			case datatype.FLOAT64:
				v := tmp.(float64)
				if v < 0 {
					v *= -1
				}
				return v, nil
			case datatype.FLOAT32:
				v := tmp.(float32)
				if v < 0 {
					v *= -1
				}
				return v, nil
			case datatype.INT64:
				v := tmp.(int64)
				if v < 0 {
					v *= -1
				}
				return v, nil
			case datatype.INT32:
				v := tmp.(int32)
				if v < 0 {
					v *= -1
				}
				return v, nil
			default:
				return nil, fmt.Errorf("unknown type")
			}
		},
	}
	return res
}

func NewSqrtFunc() *IFunc {
	res := &IFunc{
		Name: "SQRT",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in SQRT")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return nil, err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return nil, fmt.Errorf("type cann't use SQRT function")

			default:
				return math.Sqrt(datatype.ToFloat64(tmp)), nil
			}
		},
	}
	return res
}

func NewPowFunc() *IFunc {
	res := &IFunc{
		Name: "POW",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 2 {
				return false
			}

			return es[0].IsAggregate() || es[1].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 2 {
				return float64(0), fmt.Errorf("not enough parameters in POW")
			}
			var (
				err        error
				tmp1, tmp2 interface{}
				t1         = Expressions[0]
				t2         = Expressions[1]
			)

			input.ResetIndex()
			if tmp1, err = t1.Result(input); err != nil {
				return float64(0), err
			}
			input.ResetIndex()
			if tmp2, err = t2.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp1) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use POW function")

			default:
				switch datatype.TypeOf(tmp2) {
				case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
					return float64(0), fmt.Errorf("type cann't use POW function")
				}
				v1, v2 := datatype.ToFloat64(tmp1), datatype.ToFloat64(tmp2)
				return math.Pow(v1, v2), nil
			}
		},
	}
	return res
}

func NewLogFunc() *IFunc {
	res := &IFunc{
		Name: "LOG",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 2 {
				return false
			}
			return es[0].IsAggregate() || es[1].IsAggregate()

		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 2 {
				return float64(0), fmt.Errorf("not enough parameters in LOG")
			}
			var (
				err        error
				tmp1, tmp2 interface{}
				t1         = Expressions[0]
				t2         = Expressions[1]
			)

			input.ResetIndex()
			if tmp1, err = t1.Result(input); err != nil {
				return float64(0), err
			}
			input.ResetIndex()
			if tmp2, err = t2.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp1) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use LOG function")

			default:
				switch datatype.TypeOf(tmp2) {
				case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
					return float64(0), fmt.Errorf("type cann't use LOG function")
				}
				v1, v2 := datatype.ToFloat64(tmp1), datatype.ToFloat64(tmp2)
				return math.Log(v1) / math.Log(v2), nil
			}
		},
	}
	return res
}

func NewLog10Func() *IFunc {
	res := &IFunc{
		Name: "LOG",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in LOG10")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use LOG10 function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Log10(v), nil
			}
		},
	}
	return res
}

func NewLog2Func() *IFunc {
	res := &IFunc{
		Name: "LOG",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in LOG10")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use LOG10 function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Log2(v), nil
			}
		},
	}
	return res
}

func NewLnFunc() *IFunc {
	res := &IFunc{
		Name: "LN",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in LOG10")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use LOG10 function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Log(v), nil
			}
		},
	}
	return res
}

func NewCeilFunc() *IFunc {
	res := &IFunc{
		Name: "CEIL",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in CEIL")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use CEIL function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Ceil(v), nil
			}
		},
	}
	return res
}

func NewFloorFunc() *IFunc {
	res := &IFunc{
		Name: "FLOOR",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in FLOOR")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use FLOOR function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Floor(v), nil
			}
		},
	}
	return res
}

func NewRoundFunc() *IFunc {
	res := &IFunc{
		Name: "ROUND",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in ROUND")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use ROUND function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Round(v), nil
			}
		},
	}
	return res
}

func NewSinFunc() *IFunc {
	res := &IFunc{
		Name: "SIN",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in SIN")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use SIN function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Sin(v), nil
			}
		},
	}
	return res
}

func NewCosFunc() *IFunc {
	res := &IFunc{
		Name: "COS",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in COS")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use COS function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Cos(v), nil
			}
		},
	}
	return res
}

func NewTanFunc() *IFunc {
	res := &IFunc{
		Name: "TAN",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in TAN")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use TAN function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Tan(v), nil
			}
		},
	}
	return res
}

func NewASinFunc() *IFunc {
	res := &IFunc{
		Name: "ASIN",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in ASIN")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use ASIN function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Asin(v), nil
			}
		},
	}
	return res
}

func NewACosFunc() *IFunc {
	res := &IFunc{
		Name: "ACOS",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in ACOS")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use ACOS function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Acos(v), nil
			}
		},
	}
	return res
}

func NewATanFunc() *IFunc {
	res := &IFunc{
		Name: "ATAN",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in ATAN")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use ATAN function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Atan(v), nil
			}
		},
	}
	return res
}

func NewSinhFunc() *IFunc {
	res := &IFunc{
		Name: "SINH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in SINH")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use SINH function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Sinh(v), nil
			}
		},
	}
	return res
}

func NewCoshFunc() *IFunc {
	res := &IFunc{
		Name: "COSH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in COSH")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use COSH function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Cosh(v), nil
			}
		},
	}
	return res
}

func NewTanhFunc() *IFunc {
	res := &IFunc{
		Name: "TANH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in TANH")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use TANH function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Tanh(v), nil
			}
		},
	}
	return res
}

func NewASinhFunc() *IFunc {
	res := &IFunc{
		Name: "ASINH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in ASINH")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use ASINH function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Asinh(v), nil
			}
		},
	}
	return res
}

func NewACoshFunc() *IFunc {
	res := &IFunc{
		Name: "ACOSH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in ACOSH")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use ACOSH function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Acosh(v), nil
			}
		},
	}
	return res
}

func NewATanhFunc() *IFunc {
	res := &IFunc{
		Name: "ATANH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return float64(0), fmt.Errorf("not enough parameters in ATANH")
			}
			var (
				err error
				tmp interface{}
				t   = Expressions[0]
			)

			input.ResetIndex()
			if tmp, err = t.Result(input); err != nil {
				return float64(0), err
			}

			switch datatype.TypeOf(tmp) {
			case datatype.STRING, datatype.BOOL, datatype.TIMESTAMP:
				return float64(0), fmt.Errorf("type cann't use ATANH function")

			default:
				v := datatype.ToFloat64(tmp)
				return math.Atanh(v), nil
			}
		},
	}
	return res
}

func NewRandomFunc() *IFunc {
	res := &IFunc{
		Name: "RANDOM",
		IsAggregate: func(es []*ExpressionNode) bool {
			return false
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			return rand.Float64(), nil
		},
	}
	return res
}

func NewEFunc() *IFunc {
	res := &IFunc{
		Name: "E",
		IsAggregate: func(es []*ExpressionNode) bool {
			return false
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			return math.E, nil
		},
	}
	return res
}

func NewPiFunc() *IFunc {
	res := &IFunc{
		Name: "PI",
		IsAggregate: func(es []*ExpressionNode) bool {
			return false
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.FLOAT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			return math.Pi, nil
		},
	}
	return res
}
