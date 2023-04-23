package connector

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/connector/file"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/partition"
	"github.com/gotodb/gotodb/pkg/likematcher"
	"github.com/gotodb/gotodb/plan/operator"
	"github.com/gotodb/gotodb/row"
	"io"
)

type Connector interface {
	GetMetadata() (*metadata.Metadata, error)
	GetPartition(parallelNumber int) (*partition.Partition, error)
	GetReader(file *partition.FileLocation, selectedMD *metadata.Metadata, filters []*operator.BooleanExpressionNode) (row.GroupReader, error)
	Insert(rb *row.RowsBuffer, Columns []string) (affectedRows int64, err error)
	ShowTables(catalog, schema string, like, escape *string) row.Reader
	ShowSchemas(catalog string, like, escape *string) row.Reader
	ShowColumns(catalog, schema, table string) row.Reader
	ShowPartitions(catalog, schema, table string) row.Reader
}

func NewConnector(catalog string, schema string, table string) (Connector, error) {
	switch catalog {
	case "test":
		return NewTestConnector(catalog, schema, table)
	case "file":
		return file.NewFileConnector(catalog, schema, table)
	case "http":
		return NewHttpConnector(catalog, schema, table)
	case "mysql":
		return NewMysqlConnector(catalog, schema, table)
	}
	return nil, fmt.Errorf("newConnector failed: table %s.%s.%s not found", catalog, schema, table)
}

func NewEmptyConnector(catalog string) Connector {
	switch catalog {
	case "test":
		return NewTestConnectorEmpty()
	case "file":
		return file.NewFileConnectorEmpty()
	case "http":
		return NewHttpConnectorEmpty()
	case "mysql":
		return NewMysqlConnectorEmpty()
	default:
		return NewTestConnectorEmpty()
	}
}

func ShowCatalogs(like, escape *string) row.Reader {
	var err error
	catalogs := map[string]struct{}{}
	for _, c := range config.Conf.FileConnectors {
		catalogs[c.Catalog] = struct{}{}
	}

	for _, c := range config.Conf.HttpConnectors {
		catalogs[c.Catalog] = struct{}{}
	}

	for _, c := range config.Conf.MysqlConnectors {
		catalogs[c.Catalog] = struct{}{}
	}

	var rs []*row.Row

	matcher, err := likematcher.Compile(*like, *escape)
	if err == nil {
		for catalog := range catalogs {
			if matcher.Match([]byte(catalog)) {
				r := row.NewRow()
				r.AppendVals(catalog)
				rs = append(rs, r)
			}
		}
	}

	i := 0

	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		if i >= len(rs) {
			return nil, io.EOF
		}
		i++
		return rs[i-1], nil
	}
}
