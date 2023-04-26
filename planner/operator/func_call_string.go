package operator

import (
	"fmt"
	"strings"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

func NewLengthFunc() *IFunc {
	res := &IFunc{
		Name: "LENGTH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.INT64, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in LENGTH")
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
			case datatype.STRING:
				return int64(len(tmp.(string))), nil

			default:
				return nil, fmt.Errorf("type cann't use LENGTH function")
			}
		},
	}
	return res
}

func NewLowerFunc() *IFunc {
	res := &IFunc{
		Name: "LOWER",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.STRING, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in LOWER")
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
			case datatype.STRING:
				return strings.ToLower(tmp.(string)), nil

			default:
				return nil, fmt.Errorf("type cann't use LOWER function")
			}
		},
	}
	return res
}

func NewUpperFunc() *IFunc {
	res := &IFunc{
		Name: "UPPER",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.STRING, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in UPPER")
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
			case datatype.STRING:
				return strings.ToUpper(tmp.(string)), nil

			default:
				return nil, fmt.Errorf("type cann't use UPPER function")
			}
		},
	}
	return res
}

func NewReverseFunc() *IFunc {
	res := &IFunc{
		Name: "REVERSE",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.STRING, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in REVERSE")
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
			case datatype.STRING:
				bs := []byte(tmp.(string))
				bd := make([]byte, len(bs))
				for i := 0; i < len(bs); i++ {
					bd[len(bs)-i-1] = bs[i]
				}
				return string(bd), nil

			default:
				return nil, fmt.Errorf("type cann't use REVERSE function")
			}
		},
	}
	return res
}

func NewConcatFunc() *IFunc {
	res := &IFunc{
		Name: "CONCAT",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 2 {
				return false
			}
			return es[0].IsAggregate() || es[1].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.STRING, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 2 {
				return nil, fmt.Errorf("not enough parameters in CONCAT")
			}
			var (
				err        error
				tmp1, tmp2 interface{}
				t1         = Expressions[0]
				t2         = Expressions[1]
			)

			input.ResetIndex()
			if tmp1, err = t1.Result(input); err != nil {
				return nil, err
			}

			input.ResetIndex()
			if tmp2, err = t2.Result(input); err != nil {
				return nil, err
			}

			if datatype.TypeOf(tmp1) != datatype.STRING || datatype.TypeOf(tmp2) != datatype.STRING {
				return nil, fmt.Errorf("type error in CONCAT")
			}

			return tmp1.(string) + tmp2.(string), nil
		},
	}
	return res
}

func NewSubstrFunc() *IFunc {
	res := &IFunc{
		Name: "SUBSTR",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 3 {
				return false
			}
			return es[0].IsAggregate() || es[1].IsAggregate() || es[2].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.STRING, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 3 {
				return nil, fmt.Errorf("not enough parameters in SUBSTR")
			}
			var (
				err              error
				tmp1, tmp2, tmp3 interface{}
				t1               = Expressions[0]
				t2               = Expressions[1]
				t3               = Expressions[2]
			)

			input.ResetIndex()
			if tmp1, err = t1.Result(input); err != nil {
				return nil, err
			}

			input.ResetIndex()
			if tmp2, err = t2.Result(input); err != nil {
				return nil, err
			}

			input.ResetIndex()
			if tmp3, err = t3.Result(input); err != nil {
				return nil, err
			}

			bgn, end := datatype.ToInt64(tmp2), datatype.ToInt64(tmp3)

			if datatype.TypeOf(tmp1) != datatype.STRING {
				return nil, fmt.Errorf("type error in SUBSTR")
			}
			if bgn < 0 || end < 0 {
				return nil, fmt.Errorf("index out of range in SUBSTR")
			}
			return tmp1.(string)[bgn:end], nil

		},
	}
	return res
}

func NewReplaceFunc() *IFunc {
	res := &IFunc{
		Name: "REPLACE",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 3 {
				return false
			}
			return es[0].IsAggregate() || es[1].IsAggregate() || es[2].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.STRING, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 3 {
				return nil, fmt.Errorf("not enough parameters in REPLACE")
			}
			var (
				err              error
				tmp1, tmp2, tmp3 interface{}
				t1               = Expressions[0]
				t2               = Expressions[1]
				t3               = Expressions[2]
			)

			input.ResetIndex()
			if tmp1, err = t1.Result(input); err != nil {
				return nil, err
			}

			input.ResetIndex()
			if tmp2, err = t2.Result(input); err != nil {
				return nil, err
			}

			input.ResetIndex()
			if tmp3, err = t3.Result(input); err != nil {
				return nil, err
			}

			if datatype.TypeOf(tmp1) != datatype.STRING || datatype.TypeOf(tmp2) != datatype.STRING || datatype.TypeOf(tmp3) != datatype.STRING {
				return nil, fmt.Errorf("type error in REPLACE")
			}

			return strings.Replace(tmp1.(string), tmp2.(string), tmp3.(string), -1), nil

		},
	}
	return res
}
