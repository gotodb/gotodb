package connector

import (
	"encoding/json"
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"
	"io"
	"net/http"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Http struct {
	Config        *config.HttpConnector
	Metadata      *metadata.Metadata
	FileType      filesystem.FileType
	PartitionInfo *partition.Info
}

func NewHttpConnectorEmpty() *Http {
	return &Http{}
}

func NewHttpConnector(catalog, schema, table string) (*Http, error) {
	var err error
	res := &Http{}
	key := strings.Join([]string{catalog, schema, table}, ".")
	conf := config.Conf.HttpConnectors.GetConfig(key)
	if conf == nil {
		return nil, fmt.Errorf("http connector: table not found")
	}
	res.Config = conf
	res.FileType = filesystem.HTTP
	res.Metadata, err = NewHttpMetadata(conf)

	return res, err
}

func NewHttpMetadata(conf *config.HttpConnector) (*metadata.Metadata, error) {
	res := metadata.NewMetadata()
	for i := 0; i < len(conf.ColumnNames); i++ {
		col := &metadata.ColumnMetadata{
			Catalog:    conf.Catalog,
			Schema:     conf.Schema,
			Table:      conf.Table,
			ColumnName: conf.ColumnNames[i],
			ColumnType: gtype.TypeNameToType(conf.ColumnTypes[i]),
		}
		res.AppendColumn(col)
	}

	res.Reset()
	return res, nil
}

func (c *Http) GetMetadata() (*metadata.Metadata, error) {
	return c.Metadata, nil
}

func (c *Http) GetPartitionInfo() (*partition.Info, error) {
	if c.PartitionInfo == nil {
		c.PartitionInfo = partition.New(metadata.NewMetadata())
		c.PartitionInfo.FileList = append(c.PartitionInfo.FileList, &filesystem.FileLocation{Location: "0", FileType: c.FileType})
	}
	return c.PartitionInfo, nil
}

func (c *Http) GetReader(file *filesystem.FileLocation, md *metadata.Metadata, filters []*operator.BooleanExpressionNode) func(indexes []int) (*row.RowsGroup, error) {
	var str string
	for _, filter := range filters {
		if filter.Name == "options" {
			str = filter.Predicated.Predicate.RightValueExpression.PrimaryExpression.StringValue.Str
			break
		}
	}

	type Options struct {
		Url string `json:"url"`
	}

	var options Options

	if err := json.Unmarshal([]byte(str), &options); err != nil {
		return func(indexes []int) (*row.RowsGroup, error) {
			return nil, err
		}
	}

	resp, err := http.Get(options.Url)
	if err != nil {
		return func(indexes []int) (*row.RowsGroup, error) {
			return nil, err
		}
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return func(indexes []int) (*row.RowsGroup, error) {
			return nil, err
		}
	}

	var result interface{}
	err = json.Unmarshal(content, &result)
	if err != nil {
		return func(indexes []int) (*row.RowsGroup, error) {
			return nil, err
		}
	}

	return func(indexes []int) (*row.RowsGroup, error) {
		if result == nil {
			return nil, io.EOF
		}
		rg := row.NewRowsGroup(md.SelectColumnsByIndexes(indexes))

		for _, index := range indexes {
			col := rg.Metadata.Columns[index]
			if col.ColumnName == "options" {
				rg.Vals[index] = append(rg.Vals[index], str)
			} else if col.ColumnName == "_" {
				rg.Vals[index] = append(rg.Vals[index], string(content))
			}
		}

		switch result.(type) {
		case map[string]interface{}:
			for _, index := range indexes {
				col := rg.Metadata.Columns[index]
				if value, ok := result.(map[string]interface{})[col.ColumnName]; ok {
					rg.Vals[index] = append(rg.Vals[index], gtype.ToType(value, md.Columns[index].ColumnType))
				} else {
					rg.Vals[index] = append(rg.Vals[index], nil)
				}
			}
		case []map[string]interface{}:
			for _, r := range result.([]map[string]interface{}) {
				for _, index := range indexes {
					col := rg.Metadata.Columns[index]
					if value, ok := r[col.ColumnName]; ok {
						rg.Vals[index] = append(rg.Vals[index], gtype.ToType(value, md.Columns[index].ColumnType))
					} else {
						rg.Vals[index] = append(rg.Vals[index], nil)
					}
				}
			}
		}

		rg.RowsNumber++
		result = nil
		return rg, nil
	}
}

func (c *Http) ShowSchemas(catalog string, _, _ *string) func() (*row.Row, error) {
	var err error
	var rs []*row.Row
	for key := range config.Conf.HttpConnectors {
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

func (c *Http) ShowTables(catalog, schema string, _, _ *string) func() (*row.Row, error) {
	var err error
	var rs []*row.Row
	for key := range config.Conf.HttpConnectors {
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

func (c *Http) ShowColumns(catalog, schema, table string) func() (*row.Row, error) {
	var err error
	var rs []*row.Row
	for key, conf := range config.Conf.HttpConnectors {
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

func (c *Http) ShowPartitions(_, _, _ string) func() (*row.Row, error) {
	return func() (*row.Row, error) {
		return nil, io.EOF
	}
}
