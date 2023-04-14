package partition

import (
	"strings"
)

type FileType int32

const (
	FileTypeUnknown FileType = iota
	FileTypeCSV
	FileTypeParquet
	FileTypeORC
)

func StringToFileType(ts string) FileType {
	switch strings.ToUpper(ts) {
	case "CSV":
		return FileTypeCSV
	case "PARQUET":
		return FileTypeParquet
	case "ORC":
		return FileTypeORC
	default:
		return FileTypeUnknown
	}
}

type FileLocation struct {
	Location string
	FileType FileType
}

func NewFileLocation(loc string, ft FileType) *FileLocation {
	return &FileLocation{
		Location: loc,
		FileType: ft,
	}
}
