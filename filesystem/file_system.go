package filesystem

import (
	"fmt"
	"io"
	"strings"
)

type FileType int32

const (
	UNKNOWNFILETYPE FileType = iota
	CSV
	PARQUET
	ORC
	HTTP
)

func StringToFileType(ts string) FileType {
	switch strings.ToUpper(ts) {
	case "CSV":
		return CSV
	case "PARQUET":
		return PARQUET
	case "ORC":
		return ORC
	case "HTTP":
		return HTTP
	default:
		return UNKNOWNFILETYPE
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

type VirtualFile interface {
	io.ReaderAt
	io.ReadCloser
	io.Seeker
	Size() int64
}

var (
	fileSystems = []VirtualFileSystem{
		&LocalFileSystem{},
	}
)

type VirtualFileSystem interface {
	Accept(*FileLocation) bool
	Open(*FileLocation) (VirtualFile, error)
	List(*FileLocation) ([]*FileLocation, error)
	IsDir(*FileLocation) bool
}

func Open(filepath string) (VirtualFile, error) {
	fileLocation := &FileLocation{
		Location: filepath,
	}
	for _, fs := range fileSystems {
		if fs.Accept(fileLocation) {
			return fs.Open(fileLocation)
		}
	}
	return nil, fmt.Errorf("unknown file %s", filepath)
}

func List(filepath string) ([]*FileLocation, error) {
	fileLocation := &FileLocation{
		Location: filepath,
	}
	for _, fs := range fileSystems {
		if fs.Accept(fileLocation) {
			return fs.List(fileLocation)
		}
	}
	return nil, fmt.Errorf("unknown file %s", filepath)
}

func IsDir(filepath string) bool {
	fileLocation := &FileLocation{
		Location: filepath,
	}
	for _, fs := range fileSystems {
		if fs.Accept(fileLocation) {
			return fs.IsDir(fileLocation)
		}
	}
	return false
}
