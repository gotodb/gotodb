package filereader

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/gotodb/gotodb/config"
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

func (csv *CSV) TypeConvert(rg *row.RowsGroup) (*row.RowsGroup, error) {
	jobs := make(chan int)
	done := make(chan bool)
	cn := len(csv.Indexes)
	colTypes := make([]gtype.Type, cn)
	for i := 0; i < cn; i++ {
		colTypes[i], _ = csv.Metadata.GetTypeByIndex(csv.Indexes[i])
	}

	for i := 0; i < config.Conf.Runtime.ParallelNumber; i++ {
		go func() {
			for {
				j, ok := <-jobs
				if ok {
					for k := 0; k < cn; k++ {
						v := rg.Vals[k][j]
						cv := gtype.ToType(v, colTypes[k])
						rg.Vals[k][j] = cv
					}
				} else {
					done <- true
					break
				}
			}
		}()
	}

	for i := 0; i < rg.RowsNumber; i++ {
		jobs <- i
	}
	close(jobs)
	for i := 0; i < config.Conf.Runtime.ParallelNumber; i++ {
		<-done
	}
	return rg, nil
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
			rg.Vals[i] = append(rg.Vals[i], record[index])
		}
		rg.RowsNumber++
	}

	if err != nil {
		csv.Closer.Close()
		if err == io.EOF && rg.RowsNumber > 0 {
			err = nil
		} else {
			return nil, err
		}
	}

	return csv.TypeConvert(rg)
}
