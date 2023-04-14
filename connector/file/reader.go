package file

import (
	"fmt"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/partition"
	"github.com/gotodb/gotodb/row"
	"os"
)

type Reader interface {
	Read(indexes []int) (rg *row.RowsGroup, err error)
}

func NewReader(file *partition.FileLocation, md *metadata.Metadata) (Reader, error) {
	switch file.FileType {
	case partition.FileTypeCSV:
		osFile, err := os.Open(file.Location)
		if err != nil {
			return nil, err
		}
		return NewCSV(osFile, md), nil
	}
	return nil, fmt.Errorf("file type %d is not defined", file.FileType)
}
