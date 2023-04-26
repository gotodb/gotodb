package row

import (
	"github.com/gotodb/gotodb/datatype"
)

type Row struct {
	Keys []interface{}
	Vals []interface{}
}

type Reader func() (*Row, error)

func NewRow(vals ...interface{}) *Row {
	colNum := 0
	if vals != nil {
		colNum = len(vals)
	}
	res := &Row{
		Keys: []interface{}{},
		Vals: make([]interface{}, colNum),
	}
	for i := 0; i < colNum; i++ {
		res.Vals[i] = vals[i]
	}
	return res
}

func (r *Row) GetKeyString() string {
	res := ""
	if r.Keys == nil {
		r.Keys = []interface{}{}
	}
	for _, key := range r.Keys {
		res += datatype.ToKeyString(key) + ":"
	}
	return res
}

func (r *Row) AppendKeys(keys ...interface{}) *Row {
	r.Keys = append(r.Keys, keys...)
	return r
}

func (r *Row) AppendVals(vals ...interface{}) *Row {
	r.Vals = append(r.Vals, vals...)
	return r
}

func (r *Row) AppendRow(row *Row) *Row {
	r.Vals = append(r.Vals, row.Vals...)
	return r
}

func (r *Row) ClearKeys() {
	r.Keys = []interface{}{}
}

func (r *Row) Clear() {
	r.Keys = r.Keys[:0]
	r.Vals = r.Vals[:0]
}
