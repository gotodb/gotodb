package datatype

import (
	"io"
	"math"
)

//LittleEndian

func BinaryReadINT8(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums))
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums) != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = int8(buf[i])
	}
	return nil
}

func BinaryReadINT16(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*2)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*2 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = int16(uint16(buf[i*2+0]) |
			uint16(buf[i*2+1])<<8)
	}
	return nil
}

func BinaryReadINT32(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*4)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*4 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = int32(uint32(buf[i*4+0]) |
			uint32(buf[i*4+1])<<8 |
			uint32(buf[i*4+2])<<16 |
			uint32(buf[i*4+3])<<24)
	}
	return nil
}

func BinaryReadINT64(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*8)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*8 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = int64(uint64(buf[i*8+0]) |
			uint64(buf[i*8+1])<<8 |
			uint64(buf[i*8+2])<<16 |
			uint64(buf[i*8+3])<<24 |
			uint64(buf[i*8+4])<<32 |
			uint64(buf[i*8+5])<<40 |
			uint64(buf[i*8+6])<<48 |
			uint64(buf[i*8+7])<<56)
	}
	return nil
}

func BinaryReadUINT8(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums))
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums) != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = buf[i]
	}
	return nil
}

func BinaryReadUINT16(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*2)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*2 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = uint16(buf[i*2+0]) |
			uint16(buf[i*2+1])<<8
	}
	return nil
}

func BinaryReadUINT32(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*4)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*4 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = uint32(buf[i*4+0]) |
			uint32(buf[i*4+1])<<8 |
			uint32(buf[i*4+2])<<16 |
			uint32(buf[i*4+3])<<24
	}
	return nil
}

func BinaryReadUINT64(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*8)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*8 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = uint64(buf[i*8+0]) |
			uint64(buf[i*8+1])<<8 |
			uint64(buf[i*8+2])<<16 |
			uint64(buf[i*8+3])<<24 |
			uint64(buf[i*8+4])<<32 |
			uint64(buf[i*8+5])<<40 |
			uint64(buf[i*8+6])<<48 |
			uint64(buf[i*8+7])<<56
	}
	return nil
}

func BinaryReadFLOAT32(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*4)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*4 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = math.Float32frombits(uint32(buf[i*4+0]) |
			uint32(buf[i*4+1])<<8 |
			uint32(buf[i*4+2])<<16 |
			uint32(buf[i*4+3])<<24)
	}
	return nil
}

func BinaryReadFLOAT64(r io.Reader, nums []interface{}) error {
	buf := make([]byte, len(nums)*8)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	if len(nums)*8 != n {
		return io.ErrUnexpectedEOF
	}

	for i := 0; i < len(nums); i++ {
		nums[i] = math.Float64frombits(uint64(buf[i*8+0]) |
			uint64(buf[i*8+1])<<8 |
			uint64(buf[i*8+2])<<16 |
			uint64(buf[i*8+3])<<24 |
			uint64(buf[i*8+4])<<32 |
			uint64(buf[i*8+5])<<40 |
			uint64(buf[i*8+6])<<48 |
			uint64(buf[i*8+7])<<56)
	}
	return nil
}
