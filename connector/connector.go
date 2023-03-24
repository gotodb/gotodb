package connector

import (
	"fmt"

	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Connector interface {
	GetMetadata() (*metadata.Metadata, error)
	GetPartitionInfo() (*partition.PartitionInfo, error)
	GetReader(file *filesystem.FileLocation, md *metadata.Metadata) func(indexes []int) (*row.RowsGroup, error)

	ShowTables(catalog, schema, table string, like, escape *string) func() (*row.Row, error)
	ShowSchemas(catalog, schema, table string, like, escape *string) func() (*row.Row, error)
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
		}
	}
	return nil, fmt.Errorf("newConnector failed: table %s.%s.%s not found", catalog, schema, table)
}
