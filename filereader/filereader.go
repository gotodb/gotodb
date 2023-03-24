package filereader

import (
	"fmt"

	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type FileReader interface {
	Read(indexes []int) (rg *row.RowsGroup, err error)
}

func NewReader(file *filesystem.FileLocation, md *metadata.Metadata) (FileReader, error) {
	switch file.FileType {
	case filesystem.CSV:
		vf, err := filesystem.Open(file.Location)
		if err != nil {
			return nil, err
		}
		return New(vf, md), nil
	}
	return nil, fmt.Errorf("file type %d is not defined", file.FileType)
}
