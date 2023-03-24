package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

type CompressType int32

const (
	UNCOMPRESSED CompressType = iota
	GZIP
	SNAPPY
)

func UncompressGzip(buf []byte) ([]byte, error) {
	rbuf := bytes.NewReader(buf)
	gzipReader, _ := gzip.NewReader(rbuf)
	res, err := io.ReadAll(gzipReader)
	return res, err
}

func CompressGzip(buf []byte) []byte {
	var res bytes.Buffer
	gzipWriter := gzip.NewWriter(&res)
	gzipWriter.Write(buf)
	gzipWriter.Close()
	return res.Bytes()
}

func UncompressSnappy(buf []byte) ([]byte, error) {
	return snappy.Decode(nil, buf)
}

func CompressSnappy(buf []byte) []byte {
	return snappy.Encode(nil, buf)
}

func Uncompress(buf []byte, compressType CompressType) ([]byte, error) {
	switch compressType {
	case GZIP:
		return UncompressGzip(buf)
	case SNAPPY:
		return UncompressSnappy(buf)
	case UNCOMPRESSED:
		return buf, nil
	default:
		return nil, fmt.Errorf("unsupported compress method")
	}
}

func Compress(buf []byte, compressType CompressType) []byte {
	switch compressType {
	case GZIP:
		return CompressGzip(buf)
	case SNAPPY:
		return CompressSnappy(buf)
	case UNCOMPRESSED:
		return buf
	default:
		return nil
	}
}
