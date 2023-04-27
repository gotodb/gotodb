package connector

import (
	"fmt"
	"github.com/gotodb/gotodb/connector/file"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/partition"
	"github.com/gotodb/gotodb/row"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)

type Test struct {
	Metadata  *metadata.Metadata
	Rows      []row.Row
	Index     int64
	Table     string
	Partition *partition.Partition
}

var columns = map[string]datatype.Type{
	"process_date": datatype.TIMESTAMP,
	"var1":         datatype.INT64,
	"var2":         datatype.FLOAT64,
	"var3":         datatype.STRING,
	"data_source":  datatype.STRING,
	"network_id":   datatype.INT64,
	"event_date":   datatype.DATE,
}
var tempDir = os.TempDir()

func GenerateTestRows(columns map[string]datatype.Type) error {
	f1, err := os.Create(tempDir + "/test01.csv")
	if err != nil {
		return err
	}
	f2, err := os.Create(tempDir + "/test02.csv")
	if err != nil {
		return err
	}
	keys := make([]string, 0)
	for k := range columns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i := int64(0); i < 100; i++ {
		var res []string
		for _, name := range keys {
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

		if _, err := f1.Write([]byte(s)); err != nil {
			_ = f1.Close()
			return err
		}

		if _, err := f2.Write([]byte(s)); err != nil {
			_ = f2.Close()
			return err
		}
	}

	if err := f1.Close(); err != nil {
		return err
	}

	if err := f2.Close(); err != nil {
		return err
	}

	return nil
}

func GenerateTestMetadata(columns map[string]datatype.Type, table string) *metadata.Metadata {
	res := metadata.NewMetadata()
	keys := make([]string, 0)
	for k := range columns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range keys {
		col := metadata.NewColumnMetadata(columns[name], "test", "test", table, name)
		res.AppendColumn(col)
	}
	return res
}

func NewTestConnectorEmpty() *Test {
	return &Test{
		Metadata: GenerateTestMetadata(columns, "csv"),
	}
}

func NewTestConnector(_, _, table string) (*Test, error) {
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

func (c *Test) GetPartition(_ int) (*partition.Partition, error) {
	if c.Partition == nil {
		c.Partition = partition.New(metadata.NewMetadata())
		if c.Table == "csv" {
			c.Partition.Locations = []string{
				tempDir + "/test01.csv",
				tempDir + "/test02.csv",
			}
			c.Partition.FileTypes = []partition.FileType{
				partition.FileTypeCSV,
				partition.FileTypeCSV,
			}

			if err := GenerateTestRows(columns); err != nil {
				return nil, err
			}

		} else if c.Table == "parquet" {
			c.Partition.Locations = []string{
				tempDir + "/test.parquet",
			}
			c.Partition.FileTypes = []partition.FileType{
				partition.FileTypeParquet,
			}
		} else if c.Table == "orc" {
			c.Partition.Locations = []string{
				tempDir + "/test.orc",
			}
			c.Partition.FileTypes = []partition.FileType{
				partition.FileTypeORC,
			}
		}

	}
	return c.Partition, nil
}

func (c *Test) GetReader(f *partition.FileLocation, selectedMD *metadata.Metadata, _ []string) (row.GroupReader, error) {
	reader, err := file.NewHandler(f, c.Metadata, true)
	if err != nil {
		return nil, err
	}

	indexes := make([]int, len(selectedMD.Columns))
	for i, column := range selectedMD.Columns {
		indexes[i] = c.Metadata.ColumnMap[column.ColumnName]
	}

	return func(_ []int) (*row.RowsGroup, error) {
		return reader.Read(indexes)
	}, nil
}

func (c *Test) Insert(rb *row.RowsBuffer, Columns []string) (affectedRows int64, err error) {
	part, err := c.GetPartition(0)
	if err != nil {
		return
	}
	rand.Seed(time.Now().Unix())
	n := rand.Intn(len(c.Partition.Locations))
	writer, err := file.NewHandler(part.GetNoPartitionFiles()[n], c.Metadata, false)
	if err != nil {
		return
	}

	var indexes []int
	if len(Columns) > 0 {
		indexes = make([]int, len(Columns))
		for i, column := range Columns {
			indexes[i] = c.Metadata.ColumnMap[column]
		}
	}

	return writer.Write(rb, indexes)
}

func (c *Test) ShowSchemas(_ string, _, _ *string) row.Reader {
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

func (c *Test) ShowTables(_, _ string, _, _ *string) row.Reader {
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

func (c *Test) ShowColumns(_, _, _ string) row.Reader {
	var err error
	var rs []*row.Row
	for _, column := range c.Metadata.Columns {
		r := row.NewRow()
		r.AppendVals(column.ColumnName, column.ColumnType.String())
		rs = append(rs, r)
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

func (c *Test) ShowPartitions(_, _, _ string) row.Reader {
	var err error

	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
}
