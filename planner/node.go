package planner

import (
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Node interface {
	GetType(md *metadata.Metadata) (datatype.Type, error)
	GetColumns() ([]string, error)
	Result(input *row.RowsGroup) (interface{}, error)
	IsAggregate() bool
}
