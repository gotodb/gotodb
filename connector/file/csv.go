package file

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

const (
	ReadRowsNumber = 10000
)

type CSV struct {
	Closer      io.Closer
	Metadata    *metadata.Metadata
	Reader      *csv.Reader
	Writer      *csv.Writer
	Indexes     []int
	OutMetadata *metadata.Metadata
}

func NewCSV(osFile *os.File, md *metadata.Metadata) *CSV {
	return &CSV{
		Metadata: md,
		Reader:   csv.NewReader(osFile),
		Writer:   csv.NewWriter(osFile),
		Closer:   osFile,
	}
}

func (csv *CSV) SetColumns(indexes []int) error {
	cn := csv.Metadata.GetColumnNumber()
	if csv.Indexes == nil || len(csv.Indexes) == 0 {
		csv.Indexes = make([]int, 0)
		if indexes == nil {
			for i := 0; i < cn; i++ {
				csv.Indexes = append(csv.Indexes, i)
			}
		} else {
			csv.Indexes = indexes
		}

		for _, i := range csv.Indexes {
			if i >= cn {
				return fmt.Errorf("CSV: index out of range")
			}
		}
		csv.OutMetadata = csv.Metadata.SelectColumnsByIndexes(csv.Indexes)
	}
	return nil

}

func (csv *CSV) Read(indexes []int) (*row.RowsGroup, error) {
	var (
		err    error
		record []string
	)

	if err = csv.SetColumns(indexes); err != nil {
		return nil, err
	}

	rg := row.NewRowsGroup(csv.OutMetadata)
	for r := 0; r < ReadRowsNumber; r++ {
		if record, err = csv.Reader.Read(); err != nil {
			break
		}
		for i, index := range csv.Indexes {
			rg.Vals[i] = append(rg.Vals[i], datatype.ToValue(record[index], csv.Metadata.Columns[index].ColumnType))
		}
		rg.RowsNumber++
	}
	if err == io.EOF && rg.RowsNumber > 0 {
		err = nil
	}

	if err != nil {
		_ = csv.Closer.Close()
		return nil, err
	}

	return rg, nil
}

func (csv *CSV) Write(rb *row.RowsBuffer, indexes []int) (affectedRows int64, err error) {
	if err = csv.SetColumns(indexes); err != nil {
		return
	}

	if rb.MD.GetColumnNumber() < csv.Indexes[len(csv.Indexes)-1] {
		return 0, fmt.Errorf("column does not match")
	}

	record := make([]string, len(csv.Metadata.Columns))
	var rg *row.RowsGroup
	for {
		rg, err = rb.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		for r := 0; r < rg.RowsNumber; r++ {
			for columnNum, index := range csv.Indexes {
				record[index] = datatype.ToValue(rg.Vals[columnNum][r], datatype.STRING).(string)
			}
			if err = csv.Writer.Write(record); err != nil {
				return
			}
			affectedRows++
		}
	}

	csv.Writer.Flush()
	if err = csv.Writer.Error(); err != nil {
		return
	}

	if err = csv.Closer.Close(); err != nil {
		return
	}
	return
}
