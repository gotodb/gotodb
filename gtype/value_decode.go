package gtype

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func DecodeValue(bytesReader *bytes.Reader, t Type) ([]interface{}, error) {
	switch t {
	case BOOL:
		return DecodeBOOL(bytesReader)
	case INT8:
		return DecodeINT8(bytesReader)
	case INT16:
		return DecodeINT16(bytesReader)
	case INT32:
		return DecodeINT32(bytesReader)
	case INT64:
		return DecodeINT64(bytesReader)
	case UINT8:
		return DecodeUINT8(bytesReader)
	case UINT16:
		return DecodeUINT16(bytesReader)
	case UINT32:
		return DecodeUINT32(bytesReader)
	case UINT64:
		return DecodeUINT64(bytesReader)
	case FLOAT32:
		return DecodeFLOAT32(bytesReader)
	case FLOAT64:
		return DecodeFLOAT64(bytesReader)
	case STRING:
		return DecodeSTRING(bytesReader)
	case TIMESTAMP:
		return DecodeTIMESTAMP(bytesReader)
	case DATE:
		return DecodeDATE(bytesReader)
	}

	return []interface{}{}, fmt.Errorf("decode unknown type %s", t)
}

func DecodeBOOL(bytesReader *bytes.Reader) ([]interface{}, error) {
	var cnt int32
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	totNum := (cnt + 7) / 8
	k := 0
	for i := 0; i < int(totNum) && k < int(cnt); i++ {
		b, err := bytesReader.ReadByte()
		if err != nil {
			return res, err
		}
		for j := 0; j < 8; j++ {
			if (uint32(1<<uint32(j)) & uint32(b)) > 0 {
				res[k] = true
			} else {
				res[k] = false
			}
			k++
			if k >= int(cnt) {
				break
			}
		}
	}
	return res, nil
}

func DecodeINT8(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt int8
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadINT8(bytesReader, res)
	return res, err
}

func DecodeINT16(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt int16
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadINT16(bytesReader, res)
	return res, err
}

func DecodeINT32(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt int32
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadINT32(bytesReader, res)
	return res, err
}

func DecodeINT64(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt int64
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadINT64(bytesReader, res)
	return res, err
}

func DecodeUINT8(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt uint8
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadUINT8(bytesReader, res)
	return res, err
}

func DecodeUINT16(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt uint16
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadUINT16(bytesReader, res)
	return res, err
}

func DecodeUINT32(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt uint32
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadUINT32(bytesReader, res)
	return res, err
}

func DecodeUINT64(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt uint64
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadUINT64(bytesReader, res)
	return res, err
}

func DecodeFLOAT32(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt int32
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadFLOAT32(bytesReader, res)
	return res, err
}

func DecodeFLOAT64(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt int64
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	if cnt <= 0 {
		return res, nil
	}
	err = BinaryReadFLOAT64(bytesReader, res)
	return res, err
}

func DecodeSTRING(bytesReader *bytes.Reader) ([]interface{}, error) {
	var err error
	var cnt uint32
	if err := binary.Read(bytesReader, binary.LittleEndian, &cnt); err != nil {
		return []interface{}{}, err
	}
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		buf := make([]byte, 4)
		if _, err = bytesReader.Read(buf); err != nil {
			break
		}
		ln := binary.LittleEndian.Uint32(buf)
		if ln == 0 {
			res[i] = ""
		} else {
			cur := make([]byte, ln)
			if _, err := bytesReader.Read(cur); err != nil {
				return res, err
			}
			res[i] = string(cur)
		}
	}
	return res, err
}

func DecodeTIMESTAMP(bytesReader *bytes.Reader) ([]interface{}, error) {
	nums, err := DecodeINT64(bytesReader)
	if err != nil {
		return nums, err
	}
	res := make([]interface{}, len(nums))
	for i := 0; i < len(nums); i++ {
		res[i] = Timestamp{Sec: nums[i].(int64)}
	}
	return res, nil
}

func DecodeDATE(bytesReader *bytes.Reader) ([]interface{}, error) {
	nums, err := DecodeINT64(bytesReader)
	if err != nil {
		return nums, err
	}
	res := make([]interface{}, len(nums))
	for i := 0; i < len(nums); i++ {
		res[i] = Date{Sec: nums[i].(int64)}
	}
	return res, nil
}
