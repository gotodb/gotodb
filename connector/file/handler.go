package file

import (
	"fmt"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/partition"
	"github.com/gotodb/gotodb/row"
	"os"
)

type Handler interface {
	Read(indexes []int) (rg *row.RowsGroup, err error)
	Write(rb *row.RowsBuffer, indexes []int) (affectedRows int64, err error)
}

func NewHandler(file *partition.FileLocation, md *metadata.Metadata, readonly bool) (Handler, error) {
	switch file.FileType {
	case partition.FileTypeCSV:
		var osFile *os.File
		var err error
		if readonly {
			osFile, err = os.Open(file.Location)
		} else {
			osFile, err = os.OpenFile(file.Location, os.O_RDWR|os.O_APPEND, 644)
		}
		if err != nil {
			return nil, err
		}
		return NewCSV(osFile, md), nil
	}
	return nil, fmt.Errorf("file type %d is not defined", file.FileType)
}
