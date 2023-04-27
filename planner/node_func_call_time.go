package planner

import (
	"fmt"
	"time"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

func NewNowFunc() *IFunc {
	res := &IFunc{
		Name: "NOW",
		IsAggregate: func(es []*ExpressionNode) bool {
			return false
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.TIMESTAMP, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			return time.Now(), nil
		},
	}
	return res
}

func NewDayFunc() *IFunc {
	res := &IFunc{
		Name: "DAY",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.INT32, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in DAY")
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
			case datatype.TIMESTAMP:
				return int32(tmp.(time.Time).Day()), nil
			default:
				return nil, fmt.Errorf("type cann't use DAY function")
			}
		},
	}
	return res
}

func NewMonthFunc() *IFunc {
	res := &IFunc{
		Name: "MONTH",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.INT32, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in MONTH")
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
			case datatype.TIMESTAMP:
				return int32(tmp.(time.Time).Month()), nil
			default:
				return nil, fmt.Errorf("type cann't use MONTH function")
			}
		},
	}
	return res
}

func NewYearFunc() *IFunc {
	res := &IFunc{
		Name: "YEAR",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.INT32, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in YEAR")
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
			case datatype.TIMESTAMP:
				return int32(tmp.(time.Time).Year()), nil
			default:
				return nil, fmt.Errorf("type cann't use YEAR function")
			}
		},
	}
	return res
}

func NewHourFunc() *IFunc {
	res := &IFunc{
		Name: "HOUR",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.INT32, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in HOUR")
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
			case datatype.TIMESTAMP:
				return int32(tmp.(time.Time).Hour()), nil
			default:
				return nil, fmt.Errorf("type cann't use HOUR function")
			}
		},
	}
	return res
}

func NewMinuteFunc() *IFunc {
	res := &IFunc{
		Name: "MINUTE",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.INT32, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in MINUTE")
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
			case datatype.TIMESTAMP:
				return int32(tmp.(time.Time).Minute()), nil
			default:
				return nil, fmt.Errorf("type cann't use MINUE function")
			}
		},
	}
	return res
}

func NewSecondFunc() *IFunc {
	res := &IFunc{
		Name: "SECOND",
		IsAggregate: func(es []*ExpressionNode) bool {
			if len(es) < 1 {
				return false
			}
			return es[0].IsAggregate()
		},

		GetType: func(md *metadata.Metadata, es []*ExpressionNode) (datatype.Type, error) {
			return datatype.INT32, nil
		},

		Result: func(input *row.RowsGroup, sq *datatype.QuantifierType, Expressions []*ExpressionNode) (interface{}, error) {
			if len(Expressions) < 1 {
				return nil, fmt.Errorf("not enough parameters in SECOND")
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
			case datatype.TIMESTAMP:
				return int32(tmp.(time.Time).Second()), nil
			default:
				return nil, fmt.Errorf("type cann't use SECOND function")
			}
		},
	}
	return res
}
