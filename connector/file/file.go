package file

import (
	"fmt"
	"github.com/gotodb/gotodb/partition"
	"io"
	"os"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/plan/operator"
	"github.com/gotodb/gotodb/row"
)

type File struct {
	Config        *config.FileConnector
	Metadata      *metadata.Metadata
	FileType      partition.FileType
	PartitionInfo *partition.Info
}

func NewFileConnectorEmpty() *File {
	return &File{}
}

func NewFileConnector(catalog, schema, table string) (*File, error) {
	var err error
	res := &File{}
	key := strings.Join([]string{catalog, schema, table}, ".")
	conf := config.Conf.FileConnectors.GetConfig(key)
	if conf == nil {
		return nil, fmt.Errorf("file connector: table not found")
	}
	res.Config = conf
	res.FileType = partition.StringToFileType(conf.FileType)
	res.Metadata, err = NewFileMetadata(conf)

	return res, err
}

func NewFileMetadata(conf *config.FileConnector) (*metadata.Metadata, error) {
	res := metadata.NewMetadata()
	for i := 0; i < len(conf.ColumnNames); i++ {
		col := &metadata.ColumnMetadata{
			Catalog:    conf.Catalog,
			Schema:     conf.Schema,
			Table:      conf.Table,
			ColumnName: conf.ColumnNames[i],
			ColumnType: gtype.NameToType(conf.ColumnTypes[i]),
		}
		res.AppendColumn(col)
	}

	res.Reset()
	return res, nil
}

func (c *File) GetMetadata() (*metadata.Metadata, error) {
	return c.Metadata, nil
}

func (c *File) GetPartitionInfo(_ int) (*partition.Info, error) {
	if c.PartitionInfo == nil {
		c.PartitionInfo = partition.New(metadata.NewMetadata())
		for _, loc := range c.Config.Paths {
			files, err := os.ReadDir(loc)
			if err != nil {
				return nil, err
			}
			for _, file := range files {
				c.PartitionInfo.Locations = append(c.PartitionInfo.Locations, loc+"/"+file.Name())
				c.PartitionInfo.FileTypes = append(c.PartitionInfo.FileTypes, c.FileType)
			}
		}
	}
	return c.PartitionInfo, nil
}

func (c *File) GetReader(file *partition.FileLocation, md *metadata.Metadata, _ []*operator.BooleanExpressionNode) (row.GroupReader, error) {
	reader, err := NewReader(file, md)
	if err != nil {
		return nil, err
	}
	return func(indexes []int) (*row.RowsGroup, error) {
		return reader.Read(indexes)
	}, nil
}

func (c *File) ShowSchemas(catalog string, _, _ *string) row.Reader {
	var err error
	var rs []*row.Row
	for key := range config.Conf.FileConnectors {
		ns := strings.Split(key, ".")
		c, s, _ := ns[0], ns[1], ns[2]
		if c == catalog {
			r := row.NewRow()
			r.AppendVals(s)
			rs = append(rs, r)
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

func (c *File) ShowTables(catalog, schema string, _, _ *string) row.Reader {
	var err error
	var rs []*row.Row
	for key := range config.Conf.FileConnectors {
		ns := strings.Split(key, ".")
		c, s, t := ns[0], ns[1], ns[2]
		if c == catalog && s == schema {
			r := row.NewRow()
			r.AppendVals(t)
			rs = append(rs, r)
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

func (c *File) ShowColumns(catalog, schema, table string) row.Reader {
	var err error
	var rs []*row.Row
	for key, conf := range config.Conf.FileConnectors {
		ns := strings.Split(key, ".")
		c, s, t := ns[0], ns[1], ns[2]
		if c == catalog && s == schema && t == table {
			for i, name := range conf.ColumnNames {
				r := row.NewRow()
				r.AppendVals(name, conf.ColumnTypes[i])
				rs = append(rs, r)
			}
			break
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

func (c *File) ShowPartitions(_, _, _ string) row.Reader {
	return func() (*row.Row, error) {
		return nil, io.EOF
	}
}
