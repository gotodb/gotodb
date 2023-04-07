package connector

import (
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"
	"io"
	"os"
	"strings"
	"time"

	"github.com/gotodb/gotodb/filereader"
	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Test struct {
	Metadata      *metadata.Metadata
	Rows          []row.Row
	Index         int64
	Table         string
	PartitionInfo *partition.Info
}

var columns = []string{"process_date", "var1", "var2", "var3", "data_source", "network_id", "event_date"}
var tempDir = os.TempDir()

func GenerateTestRows(columns []string) error {
	f1, err := os.Create(tempDir + "/test01.csv")
	if err != nil {
		return err
	}
	f2, err := os.Create(tempDir + "/test02.csv")
	if err != nil {
		return err
	}
	defer f1.Close()
	defer f2.Close()

	for i := int64(0); i < int64(100); i++ {
		var res []string
		for _, name := range columns {
			switch name {
			case "process_date":
				res = append(res, fmt.Sprintf("%v", time.Now().Format("2006-01-02 15:04:05")))
			case "var1":
				res = append(res, fmt.Sprintf("%v", i))
			case "var2":
				res = append(res, fmt.Sprintf("%v", float64(i)))
			case "var3":
				res = append(res, fmt.Sprintf("%v", "var3"))
			case "network_id":
				res = append(res, fmt.Sprintf("%v", i))
			case "data_source":
				res = append(res, fmt.Sprintf("data_source%v", i))
			case "event_date":
				res = append(res, fmt.Sprintf("%v", time.Now().Format("2006-01-02 15:04:05")))
			}
		}
		s := strings.Join(res, ",") + "\n"
		f1.Write([]byte(s))
		f2.Write([]byte(s))
	}
	return nil
}

func GenerateTestMetadata(columns []string, table string) *metadata.Metadata {
	res := metadata.NewMetadata()
	for _, name := range columns {
		t := gtype.UNKNOWNTYPE
		switch name {
		case "process_date":
			t = gtype.TIMESTAMP
		case "var1":
			t = gtype.INT64
		case "var2":
			t = gtype.FLOAT64
		case "var3":
			t = gtype.STRING
		case "data_source":
			t = gtype.STRING
		case "network_id":
			t = gtype.INT64
		case "event_date":
			t = gtype.DATE
		}
		col := metadata.NewColumnMetadata(t, "test", "test", table, name)
		res.AppendColumn(col)
	}
	return res
}

func NewTestConnectorEmpty() *Test {
	return &Test{}
}

func NewTestConnector(catalog, schema, table string) (*Test, error) {
	var res *Test
	res = &Test{
		Metadata: GenerateTestMetadata(columns, table),
		Index:    0,
		Table:    table,
	}
	return res, nil
}

func (c *Test) GetMetadata() (*metadata.Metadata, error) {
	return c.Metadata, nil
}

func (c *Test) GetPartitionInfo() (*partition.Info, error) {
	if c.PartitionInfo == nil {
		c.PartitionInfo = partition.New(metadata.NewMetadata())
		if c.Table == "csv" {
			c.PartitionInfo.FileList = []*filesystem.FileLocation{
				{
					Location: tempDir + "/test01.csv",
					FileType: filesystem.CSV,
				},
				{
					Location: tempDir + "/test02.csv",
					FileType: filesystem.CSV,
				},
			}
			GenerateTestRows(columns)

		} else if c.Table == "parquet" {
			c.PartitionInfo.FileList = []*filesystem.FileLocation{
				{
					Location: tempDir + "/test.parquet",
					FileType: filesystem.PARQUET,
				},
			}
		} else if c.Table == "orc" {
			c.PartitionInfo.FileList = []*filesystem.FileLocation{
				{
					Location: tempDir + "/test.orc",
					FileType: filesystem.ORC,
				},
			}
		}

	}
	return c.PartitionInfo, nil
}

func (c *Test) GetReader(file *filesystem.FileLocation, md *metadata.Metadata, filters []*operator.BooleanExpressionNode) func(indexes []int) (*row.RowsGroup, error) {
	reader, err := filereader.NewReader(file, md)
	return func(indexes []int) (*row.RowsGroup, error) {
		if err != nil {
			return nil, err
		}
		return reader.Read(indexes)
	}
}

func (c *Test) ShowTables(_, _ string, _, _ *string) func() (*row.Row, error) {
	var err error
	tables := []string{"csv", "parquet", "orc"}
	var rs []*row.Row
	for _, table := range tables {
		r := row.NewRow()
		r.AppendVals(table)
		rs = append(rs, r)
	}
	i := 0
	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		if i >= len(tables) {
			return nil, io.EOF
		}
		i++
		return rs[i-1], nil
	}
}

func (c *Test) ShowSchemas(_ string, _, _ *string) func() (*row.Row, error) {
	var err error

	r := row.NewRow()
	r.AppendVals("test")
	i := 0
	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		if i > 0 {
			return nil, io.EOF
		}
		i++
		return r, nil
	}
}

func (c *Test) ShowColumns(_, _, _ string) func() (*row.Row, error) {
	var err error
	var rs []*row.Row
	r := row.NewRow()
	r.AppendVals("ID", "INT64")
	rs = append(rs, r)

	r = row.NewRow()
	r.AppendVals("INT64", "INT64")
	rs = append(rs, r)

	r = row.NewRow()
	r.AppendVals("FLOAT64", "FLOAT64")
	rs = append(rs, r)

	r = row.NewRow()
	r.AppendVals("STRING", "STRING")
	rs = append(rs, r)

	r = row.NewRow()
	r.AppendVals("TIMESTAMP", "TIMESTAMP")
	rs = append(rs, r)

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

func (c *Test) ShowPartitions(_, _, _ string) func() (*row.Row, error) {
	var err error

	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
}
