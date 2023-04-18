package util

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/snappy"
	"io"
	"math"

	"github.com/vmihailenco/msgpack"
)

const (
	MessageEOF = math.MinInt32
	BufferSize = 1024 * 512
)

func CopyBuffer(src io.Reader, dst io.Writer) (err error) {
	buf := make([]byte, BufferSize)

	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			err = er
			break
		}
	}
	return err
}

func ReadMessage(reader io.Reader) (res []byte, err error) {
	var length int32
	err = binary.Read(reader, binary.LittleEndian, &length)
	if err == io.EOF || length == MessageEOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}

	res = make([]byte, length)
	var n int
	n, err = io.ReadFull(reader, res)
	if err == io.EOF {
		return nil, fmt.Errorf("unexpected EOF when reading message, expected read %v, only read %v", length, n)
	}
	if err != nil {
		return nil, err
	}

	buf, err := snappy.Decode(nil, res)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func WriteMessage(writer io.Writer, msg []byte) (err error) {
	buf := snappy.Encode(nil, msg)
	if err = binary.Write(writer, binary.LittleEndian, int32(len(buf))); err != nil {
		return err
	}
	if _, err = writer.Write(buf); err != nil {
		return err
	}
	return nil
}

func WriteEOFMessage(writer io.Writer) (err error) {
	if err = binary.Write(writer, binary.LittleEndian, int32(MessageEOF)); err != nil {
		return err
	}
	return nil
}

func ReadObject(reader io.Reader, obj interface{}) error {
	msg, err := ReadMessage(reader)
	if err != nil {
		return err
	}
	err = msgpack.Unmarshal(msg, obj)
	return err
}

func WriteObject(writer io.Writer, obj interface{}) error {
	buf, err := msgpack.Marshal(obj)
	if err != nil {
		return err
	}
	return WriteMessage(writer, buf)
}
