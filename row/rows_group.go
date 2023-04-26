package row

import (
	"io"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
)

type RowsGroup struct {
	Metadata   *metadata.Metadata
	RowsNumber int
	Keys       [][]interface{}
	Vals       [][]interface{}
	Index      int
}

type GroupReader func(indexes []int) (*RowsGroup, error)

func NewRowsGroup(md *metadata.Metadata) *RowsGroup {
	return &RowsGroup{
		Metadata:   md,
		RowsNumber: 0,
		Keys:       make([][]interface{}, md.GetKeyNumber()),
		Vals:       make([][]interface{}, md.GetColumnNumber()),
		Index:      0,
	}
}

func (rg *RowsGroup) Read() (*Row, error) {
	if rg.Index >= rg.RowsNumber {
		return nil, io.EOF
	}
	r := NewRow()
	for i := 0; i < len(rg.Vals); i++ {
		r.AppendVals(rg.Vals[i][rg.Index])
	}
	for i := 0; i < len(rg.Keys); i++ {
		r.AppendKeys(rg.Keys[i][rg.Index])
	}
	rg.Index++
	return r, nil
}

func (rg *RowsGroup) Write(r *Row) {
	for i, v := range r.Vals {
		rg.Vals[i] = append(rg.Vals[i], v)
	}
	for i, v := range r.Keys {
		rg.Keys[i] = append(rg.Keys[i], v)
	}
	rg.RowsNumber++
}

func (rg *RowsGroup) AppendRowVals(vals ...interface{}) {
	for i := 0; i < len(rg.Vals); i++ {
		rg.Vals[i] = append(rg.Vals[i], vals[i])
	}
	rg.RowsNumber += 1
}

func (rg *RowsGroup) AppendRowKeys(keys ...interface{}) {
	for i := 0; i < len(rg.Keys); i++ {
		rg.Keys[i] = append(rg.Keys[i], keys[i])
	}
}

func (rg *RowsGroup) AppendRowGroupRows(_rg *RowsGroup) {
	for i := 0; i < len(rg.Vals); i++ {
		rg.Vals[i] = append(rg.Vals[i], _rg.Vals[i]...)
	}
	for i := 0; i < len(rg.Keys); i++ {
		rg.Keys[i] = append(rg.Keys[i], _rg.Keys[i]...)
	}
	rg.RowsNumber += _rg.GetRowsNumber()
}

func (rg *RowsGroup) AppendRowGroupColumns(_rg *RowsGroup) {
	rg.RowsNumber = _rg.GetRowsNumber()
	for i := 0; i < _rg.GetColumnsNumber(); i++ {
		rg.Metadata.AppendColumn(_rg.Metadata.Columns[i])
		rg.Vals = append(rg.Vals, _rg.Vals[i])
	}
}

func (rg *RowsGroup) GetRowVals(ri int) []interface{} {
	res := make([]interface{}, len(rg.Vals))
	for i := 0; i < len(rg.Vals); i++ {
		res[i] = rg.Vals[i][ri]
	}
	return res
}

func (rg *RowsGroup) GetRowKeys(ri int) []interface{} {
	res := make([]interface{}, len(rg.Keys))
	for i := 0; i < len(rg.Keys); i++ {
		res[i] = rg.Keys[i][ri]
	}
	return res
}

func (rg *RowsGroup) GetRow(ri int) *Row {
	if ri >= rg.RowsNumber {
		return nil
	}

	res := Pool.Get().(*Row)
	res.Clear()

	for i := 0; i < len(rg.Vals); i++ {
		res.AppendVals(rg.Vals[i][ri])
	}
	for i := 0; i < len(rg.Keys); i++ {
		res.AppendKeys(rg.Keys[i][ri])
	}
	return res
}

func (rg *RowsGroup) ResetIndex() {
	rg.Index = 0
}

func (rg *RowsGroup) GetColumnIndex(name string) int {
	if i, ok := rg.Metadata.ColumnMap[name]; ok {
		return i
	}
	return -1
}

func (rg *RowsGroup) GetKeyString(index int) string {
	res := ""
	for _, ks := range rg.Keys {
		res += datatype.ToKeyString(ks[index]) + ":"
	}
	return res
}

func (rg *RowsGroup) ClearRows() {
	rg.Index = 0
	rg.RowsNumber = 0
	for i := 0; i < len(rg.Vals); i++ {
		rg.Vals[i] = rg.Vals[i][:0]
	}
	for i := 0; i < len(rg.Keys); i++ {
		rg.Keys[i] = rg.Keys[i][:0]
	}
}

func (rg *RowsGroup) ClearColumns() {
	rg.Index = 0
	rg.RowsNumber = 0
	rg.Vals = [][]interface{}{}
}

func (rg *RowsGroup) GetRowsNumber() int {
	return rg.RowsNumber
}

func (rg *RowsGroup) GetColumnsNumber() int {
	return len(rg.Vals)
}

func (rg *RowsGroup) GetKeyColumnsNumber() int {
	return len(rg.Keys)
}

func (rg *RowsGroup) AppendValColumns(cols ...[]interface{}) {
	for _, col := range cols {
		rg.Vals = append(rg.Vals, col)
		rg.RowsNumber = len(col)
	}
}

func (rg *RowsGroup) AppendKeyColumns(keys ...[]interface{}) {
	rg.Keys = append(rg.Keys, keys...)
}

func (rg *RowsGroup) SetColumn(index int, col []interface{}) {
	rg.Vals[index] = col
}
