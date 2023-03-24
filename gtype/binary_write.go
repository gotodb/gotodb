package gtype

import (
	"io"
	"math"
)

//LittleEndian

func BinaryWriteINT32(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*4)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := uint32(n.(int32))
		buf[i*4+0] = byte(v)
		buf[i*4+1] = byte(v >> 8)
		buf[i*4+2] = byte(v >> 16)
		buf[i*4+3] = byte(v >> 24)
	}
	_, err := w.Write(buf)
	return cnt, err

}

func BinaryWriteINT64(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*8)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := uint64(n.(int64))
		buf[i*8+0] = byte(v)
		buf[i*8+1] = byte(v >> 8)
		buf[i*8+2] = byte(v >> 16)
		buf[i*8+3] = byte(v >> 24)
		buf[i*8+4] = byte(v >> 32)
		buf[i*8+5] = byte(v >> 40)
		buf[i*8+6] = byte(v >> 48)
		buf[i*8+7] = byte(v >> 56)
	}
	_, err := w.Write(buf)
	return cnt, err
}

func BinaryWriteFLOAT32(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*4)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := math.Float32bits(n.(float32))
		buf[i*4+0] = byte(v)
		buf[i*4+1] = byte(v >> 8)
		buf[i*4+2] = byte(v >> 16)
		buf[i*4+3] = byte(v >> 24)
	}
	_, err := w.Write(buf)
	return cnt, err
}

func BinaryWriteFLOAT64(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*8)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := math.Float64bits(n.(float64))
		buf[i*8+0] = byte(v)
		buf[i*8+1] = byte(v >> 8)
		buf[i*8+2] = byte(v >> 16)
		buf[i*8+3] = byte(v >> 24)
		buf[i*8+4] = byte(v >> 32)
		buf[i*8+5] = byte(v >> 40)
		buf[i*8+6] = byte(v >> 48)
		buf[i*8+7] = byte(v >> 56)
	}
	_, err := w.Write(buf)
	return cnt, err
}
