package filereader

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

const (
	ReadRowsNumber = 10000
)

type CSV struct {
	Closer   io.Closer
	Metadata *metadata.Metadata
	Reader   *csv.Reader

	Indexes     []int
	OutMetadata *metadata.Metadata
}

func New(reader io.Reader, md *metadata.Metadata) *CSV {
	return &CSV{
		Metadata: md,
		Reader:   csv.NewReader(reader),
		Closer:   io.Closer(reader.(filesystem.VirtualFile)),
	}
}

func (csv *CSV) SetReadColumns(indexes []int) error {
	cn := csv.Metadata.GetColumnNumber()
	if csv.Indexes == nil || len(csv.Indexes) <= 0 {
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

	if err = csv.SetReadColumns(indexes); err != nil {
		return nil, err
	}

	rg := row.NewRowsGroup(csv.OutMetadata)
	for i := 0; i < ReadRowsNumber; i++ {
		if record, err = csv.Reader.Read(); err != nil {
			break
		}
		for i, index := range csv.Indexes {
			rg.Vals[i] = append(rg.Vals[i], gtype.ToType(record[index], csv.Metadata.Columns[index].ColumnType))
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
