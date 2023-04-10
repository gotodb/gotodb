package connector

import (
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"

	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Connector interface {
	GetMetadata() (*metadata.Metadata, error)
	GetPartitionInfo(parallelNumber int) (*partition.Info, error)
	GetReader(file *filesystem.FileLocation, md *metadata.Metadata, filters []*operator.BooleanExpressionNode) func(indexes []int) (*row.RowsGroup, error)

	ShowTables(catalog, schema string, like, escape *string) func() (*row.Row, error)
	ShowSchemas(catalog string, like, escape *string) func() (*row.Row, error)
	ShowColumns(catalog, schema, table string) func() (*row.Row, error)
	ShowPartitions(catalog, schema, table string) func() (*row.Row, error)
}

func NewConnector(catalog string, schema string, table string) (Connector, error) {
	if len(catalog) >= 4 {
		switch catalog[:4] {
		case "test":
			return NewTestConnector(catalog, schema, table)
		case "file":
			return NewFileConnector(catalog, schema, table)
		case "http":
			return NewHttpConnector(catalog, schema, table)
		}
	}
	return nil, fmt.Errorf("newConnector failed: table %s.%s.%s not found", catalog, schema, table)
}

func NewEmptyConnector(catalog string) Connector {
	switch catalog {
	case "test":
		return NewTestConnectorEmpty()
	case "file":
		return NewFileConnectorEmpty()
	case "http":
		return NewHttpConnectorEmpty()
	default:
		return NewTestConnectorEmpty()
	}
}
