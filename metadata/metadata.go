package metadata

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
)

type Metadata struct {
	Columns   []*ColumnMetadata
	Keys      []*ColumnMetadata
	ColumnMap map[string]int
}

func (m *Metadata) Reset() {
	m.ColumnMap = map[string]int{}
	for i, col := range m.Columns {
		name := col.ColumnName
		m.ColumnMap[name] = i

		name = col.Table + "." + name
		m.ColumnMap[name] = i

		name = col.Schema + "." + name
		m.ColumnMap[name] = i

		name = col.Catalog + "." + name
		m.ColumnMap[name] = i
	}
}

func (m *Metadata) GetColumnNames() []string {
	var res []string
	for _, c := range m.Columns {
		res = append(res, c.GetName())
	}
	return res
}

func (m *Metadata) GetColumnTypes() []gtype.Type {
	var res []gtype.Type
	for _, c := range m.Columns {
		res = append(res, c.ColumnType)
	}
	return res
}

func (m *Metadata) Copy() *Metadata {
	res := NewMetadata()
	for _, c := range m.Columns {
		res.Columns = append(res.Columns, c.Copy())
	}
	for _, k := range m.Keys {
		res.Keys = append(res.Keys, k.Copy())
	}
	res.Reset()
	return res
}

func (m *Metadata) Rename(name string) {
	for _, c := range m.Columns {
		c.Table = name
	}
	m.Reset()
}

func (m *Metadata) GetColumnNumber() int {
	return len(m.Columns)
}

func (m *Metadata) GetKeyNumber() int {
	return len(m.Keys)
}

func (m *Metadata) GetTypeByIndex(index int) (gtype.Type, error) {
	if index >= len(m.Columns) {
		return gtype.UNKNOWNTYPE, fmt.Errorf("index out of range")
	}
	return m.Columns[index].ColumnType, nil
}

func (m *Metadata) GetKeyTypeByIndex(index int) (gtype.Type, error) {
	if index >= len(m.Keys) {
		return gtype.UNKNOWNTYPE, fmt.Errorf("index out of range")
	}
	return m.Keys[index].ColumnType, nil
}

func (m *Metadata) GetTypeByName(name string) (gtype.Type, error) {
	index, ok := m.ColumnMap[name]
	if !ok {
		return gtype.UNKNOWNTYPE, fmt.Errorf("unknown column name: %v", name)
	}
	return m.GetTypeByIndex(index)
}

func (m *Metadata) GetIndexByName(name string) (int, error) {
	index, ok := m.ColumnMap[name]
	if !ok {
		return -1, fmt.Errorf("unknown column name: %v", name)
	}
	return index, nil
}

func (m *Metadata) AppendColumn(column *ColumnMetadata) {
	m.Columns = append(m.Columns, column)
	m.Reset()
}

func (m *Metadata) AppendKey(key *ColumnMetadata) {
	m.Keys = append(m.Keys, key)
}

func (m *Metadata) AppendKeyByType(t gtype.Type) {
	k := &ColumnMetadata{
		ColumnType: t,
	}
	m.Keys = append(m.Keys, k)
}

func (m *Metadata) ClearKeys() {
	m.Keys = []*ColumnMetadata{}
}

func (m *Metadata) DeleteColumnByIndex(index int) {
	ln := len(m.Columns)
	if index < 0 || index >= ln {
		return
	}
	m.Columns = append(m.Columns[:index], m.Columns[index+1:]...)
	m.Reset()
}

func (m *Metadata) SelectColumnsByIndexes(indexes []int) *Metadata {
	res := NewMetadata()
	rec := map[int]bool{}
	for _, i := range indexes {
		rec[i] = true
	}
	indexes = []int{}
	for index := range rec {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		res.AppendColumn(m.Columns[index].Copy())
	}
	return res
}

func (m *Metadata) SelectColumns(columns []string) *Metadata {
	res := NewMetadata()
	rec := map[int]bool{}
	for _, c := range columns {
		index, err := m.GetIndexByName(c)
		if err != nil {
			continue
		}
		rec[index] = true
	}
	var indexes []int
	for index := range rec {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		res.Columns = append(res.Columns, m.Columns[index].Copy())
	}
	res.Reset()
	return res
}

func (m *Metadata) Contains(columns []string) bool {
	for _, c := range columns {
		if _, err := m.GetIndexByName(c); err != nil {
			return false
		}
	}
	return true
}

func NewMetadata() *Metadata {
	return &Metadata{
		Columns:   []*ColumnMetadata{},
		Keys:      []*ColumnMetadata{},
		ColumnMap: map[string]int{},
	}
}

func SplitTableName(runtime *config.Runtime, name string) (catalog, schema, table string) {
	catalog, schema, table = runtime.Catalog, runtime.Schema, ""
	names := strings.Split(name, ".")
	ln := len(names)
	if ln >= 1 {
		table = names[ln-1]
	}
	if ln >= 2 {
		schema = names[ln-2]
	}
	if ln >= 3 {
		catalog = names[ln-3]
	}
	return
}

func JoinMetadata(mdl, mdr *Metadata) *Metadata {
	res := NewMetadata()
	for _, c := range mdl.Columns {
		res.Columns = append(res.Columns, c.Copy())
	}
	for _, k := range mdl.Keys {
		res.Keys = append(res.Keys, k.Copy())
	}
	for _, c := range mdr.Columns {
		res.Columns = append(res.Columns, c.Copy())
	}
	for _, k := range mdr.Keys {
		res.Keys = append(res.Keys, k.Copy())
	}
	res.Reset()
	return res
}
