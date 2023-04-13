package gtype

import (
	"bytes"
	"encoding/binary"
)

func EncodeValues(nums []interface{}, t Type) []byte {
	switch t {
	case BOOL:
		return EncodeBool(nums)
	case INT8:
		return EncodeINT8(nums)
	case INT16:
		return EncodeINT16(nums)
	case INT32:
		return EncodeINT32(nums)
	case INT64:
		return EncodeINT64(nums)
	case UINT8:
		return EncodeUINT8(nums)
	case UINT16:
		return EncodeUINT16(nums)
	case UINT32:
		return EncodeUINT32(nums)
	case UINT64:
		return EncodeUINT64(nums)
	case FLOAT32:
		return EncodeFLOAT32(nums)
	case FLOAT64:
		return EncodeFLOAT64(nums)
	case STRING:
		return EncodeString(nums)
	case TIMESTAMP:
		return EncodeTime(nums)
	case DATE:
		return EncodeDate(nums)
	}
	return []byte{}
}

func EncodeBool(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	binary.Write(bufWriter, binary.LittleEndian, int32(len(nums)))
	ln := len(nums)
	byteNum := (ln + 7) / 8
	res := make([]byte, byteNum)
	nilNum := 0
	for i := 0; i < ln; i++ {
		if nums[i] == nil {
			nilNum++
			continue
		}
		if nums[i].(bool) {
			res[i/8] = res[i/8] | (1 << uint32(i%8))
		}
	}
	bufWriter.Write(res)
	res2 := bufWriter.Bytes()

	numBufWriter := new(bytes.Buffer)
	binary.Write(numBufWriter, binary.LittleEndian, int32(len(nums)-nilNum))
	numBuf := numBufWriter.Bytes()
	for i := 0; i < len(numBuf); i++ {
		res2[i] = numBuf[i]
	}
	return res2
}

func EncodeINT8(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteINT8(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, int8(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeINT16(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteINT16(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, int16(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeINT32(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteINT32(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, int32(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeINT64(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteINT64(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, int32(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeUINT8(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteUINT8(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, uint8(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeUINT16(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteUINT16(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, uint16(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeUINT32(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteUINT32(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, uint32(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeUINT64(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteUINT64(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, uint32(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeFLOAT32(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteFLOAT32(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, int32(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeFLOAT64(nums []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	numBufWriter := new(bytes.Buffer)
	n, _ := BinaryWriteFLOAT64(numBufWriter, nums)
	binary.Write(bufWriter, binary.LittleEndian, int32(n))
	bufWriter.Write(numBufWriter.Bytes())
	return bufWriter.Bytes()
}

func EncodeString(ss []interface{}) []byte {
	bufWriter := new(bytes.Buffer)
	bufWriter.Write(ToBinaryINT32(int32(len(ss))))
	nilNum := 0
	for _, si := range ss {
		if si == nil {
			nilNum++
			continue
		}
		s := si.(string)
		ln := int32(len(s))
		bufWriter.Write(ToBinaryINT32(ln))
		if ln > 0 {
			bufWriter.Write([]byte(s))
		}
	}
	res := bufWriter.Bytes()

	numBufWriter := new(bytes.Buffer)
	numBufWriter.Write(ToBinaryINT32(int32(len(ss) - nilNum)))
	numBuf := numBufWriter.Bytes()

	for i := 0; i < len(numBuf); i++ {
		res[i] = numBuf[i]
	}
	return res
}

func EncodeTime(ts []interface{}) []byte {
	var nums []interface{}
	for _, ti := range ts {
		if ti == nil {
			nums = append(nums, nil)
		} else {
			nums = append(nums, ti.(Timestamp).Sec)
		}
	}
	return EncodeINT64(nums)
}

func EncodeDate(ts []interface{}) []byte {
	var nums []interface{}
	for _, ti := range ts {
		if ti == nil {
			nums = append(nums, nil)
		} else {
			nums = append(nums, ti.(Date).Sec)
		}
	}
	return EncodeINT64(nums)
}
