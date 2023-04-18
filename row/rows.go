package row

import (
	"sort"

	"github.com/gotodb/gotodb/gtype"
)

// Rows for sort rows
type Rows struct {
	Data  []*Row
	Order []gtype.OrderType
}

func NewRows(order []gtype.OrderType) *Rows {
	return &Rows{
		Data:  []*Row{},
		Order: order,
	}
}

func (r *Rows) Min() int {
	res := -1
	ln := len(r.Data)
	for i := 0; i < ln; i++ {
		if r.Data[i] == nil {
			continue
		}
		if res < 0 {
			res = i
		} else {
			if r.Less(i, res) {
				res = i
			}
		}
	}
	return res
}

func (r *Rows) Less(i, j int) bool {
	rowi, rowj := r.Data[i], r.Data[j]
	for k := 0; k < len(r.Order); k++ {
		vi, vj := rowi.Keys[k], rowj.Keys[k]
		if gtype.EQFunc(vi, vj).(bool) {
			continue
		}
		res := gtype.LTFunc(vi, vj).(bool)
		if r.Order[k] == gtype.DESC {
			res = !res
		}
		return res
	}
	return false
}

func (r *Rows) Swap(i, j int) {
	r.Data[i], r.Data[j] = r.Data[j], r.Data[i]
}

func (r *Rows) Len() int {
	return len(r.Data)
}

func (r *Rows) Sort() {
	sort.Sort(r)
}

func (r *Rows) Append(row *Row) {
	r.Data = append(r.Data, row)
}
