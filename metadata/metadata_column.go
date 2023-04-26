package metadata

import (
	"fmt"

	"github.com/gotodb/gotodb/datatype"
)

type ColumnMetadata struct {
	Catalog    string
	Schema     string
	Table      string
	ColumnName string
	ColumnType datatype.Type
}

func NewColumnMetadata(t datatype.Type, metrics ...string) *ColumnMetadata {
	res := &ColumnMetadata{
		Catalog:    "default",
		Schema:     "default",
		Table:      "default",
		ColumnName: "default",
		ColumnType: t,
	}
	ln := len(metrics)
	if ln >= 1 {
		res.ColumnName = metrics[ln-1]
	}
	if ln >= 2 {
		res.Table = metrics[ln-2]
	}
	if ln >= 3 {
		res.Schema = metrics[ln-3]
	}
	if ln >= 4 {
		res.Catalog = metrics[ln-4]
	}
	return res
}

func (c *ColumnMetadata) Copy() *ColumnMetadata {
	value := *c
	return &value
}

func (c *ColumnMetadata) GetName() string {
	return fmt.Sprintf("%v.%v.%v.%v", c.Catalog, c.Schema, c.Table, c.ColumnName)
}
