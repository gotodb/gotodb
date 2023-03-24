package operator

import (
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Node interface {
	GetType(md *metadata.Metadata) (gtype.Type, error)
	GetColumns() ([]string, error)
	Result(input *row.RowsGroup) (interface{}, error)
	IsAggregate() bool
}
