package datatype

import (
	"io"
	"math"
)

//LittleEndian

func BinaryWriteINT8(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums))
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := uint8(n.(int8))
		buf[i] = v
	}
	_, err := w.Write(buf)
	return cnt, err

}

func BinaryWriteINT16(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*2)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := uint16(n.(int16))
		buf[i*2+0] = byte(v)
		buf[i*2+1] = byte(v >> 8)
	}
	_, err := w.Write(buf)
	return cnt, err

}

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

func BinaryWriteUINT8(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums))
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := n.(uint8)
		buf[i] = v
	}
	_, err := w.Write(buf)
	return cnt, err

}

func BinaryWriteUINT16(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*2)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := n.(uint16)
		buf[i*2+0] = byte(v)
		buf[i*2+1] = byte(v >> 8)
	}
	_, err := w.Write(buf)
	return cnt, err

}

func BinaryWriteUINT32(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*4)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := n.(uint32)
		buf[i*4+0] = byte(v)
		buf[i*4+1] = byte(v >> 8)
		buf[i*4+2] = byte(v >> 16)
		buf[i*4+3] = byte(v >> 24)
	}
	_, err := w.Write(buf)
	return cnt, err

}

func BinaryWriteUINT64(w io.Writer, nums []interface{}) (int, error) {
	buf := make([]byte, len(nums)*8)
	cnt := 0
	for i, n := range nums {
		if n == nil {
			continue
		}
		cnt++
		v := n.(uint64)
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
