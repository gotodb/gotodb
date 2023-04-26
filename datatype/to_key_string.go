package datatype

import (
	"fmt"
	"math"
)

func ToKeyStringINT8(num int8) string {
	buf := make([]byte, 1)
	v := uint8(num)
	buf[0] = v
	return string(buf)

}

func ToKeyStringINT16(num int16) string {
	buf := make([]byte, 2)
	v := uint16(num)
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	return string(buf)

}

func ToKeyStringINT32(num int32) string {
	buf := make([]byte, 4)
	v := uint32(num)
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	return string(buf)

}

func ToKeyStringINT64(num int64) string {
	buf := make([]byte, 8)
	v := uint64(num)
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	buf[4] = byte(v >> 32)
	buf[5] = byte(v >> 40)
	buf[6] = byte(v >> 48)
	buf[7] = byte(v >> 56)
	return string(buf)
}

func ToKeyStringUINT8(num uint8) string {
	buf := make([]byte, 1)
	buf[0] = num
	return string(buf)

}

func ToKeyStringUINT16(num uint16) string {
	buf := make([]byte, 2)
	v := num
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	return string(buf)

}

func ToKeyStringUINT32(num uint32) string {
	buf := make([]byte, 4)
	v := num
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	return string(buf)

}

func ToKeyStringUINT64(num uint64) string {
	buf := make([]byte, 8)
	v := num
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	buf[4] = byte(v >> 32)
	buf[5] = byte(v >> 40)
	buf[6] = byte(v >> 48)
	buf[7] = byte(v >> 56)
	return string(buf)
}

func ToKeyStringFLOAT32(num float32) string {
	buf := make([]byte, 4)
	v := math.Float32bits(num)
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	return string(buf)
}

func ToKeyStringFLOAT64(num float64) string {
	buf := make([]byte, 8)
	v := math.Float64bits(num)
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	buf[4] = byte(v >> 32)
	buf[5] = byte(v >> 40)
	buf[6] = byte(v >> 48)
	buf[7] = byte(v >> 56)
	return string(buf)
}

func ToKeyStringBOOL(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func ToKeyStringTIMESTAMP(t Timestamp) string {
	return ToKeyStringINT64(t.Sec)
}

func ToKeyStringDATE(t Date) string {
	return ToKeyStringINT64(t.Sec)
}

func ToKeyString(n interface{}) string {
	switch n.(type) {
	case bool:
		return ToKeyStringBOOL(n.(bool))
	case int8:
		return ToKeyStringINT8(n.(int8))
	case int16:
		return ToKeyStringINT16(n.(int16))
	case int32:
		return ToKeyStringINT32(n.(int32))
	case int64:
		return ToKeyStringINT64(n.(int64))
	case uint8:
		return ToKeyStringUINT8(n.(uint8))
	case uint16:
		return ToKeyStringUINT16(n.(uint16))
	case uint32:
		return ToKeyStringUINT32(n.(uint32))
	case uint64:
		return ToKeyStringUINT64(n.(uint64))
	case float32:
		return ToKeyStringFLOAT32(n.(float32))
	case float64:
		return ToKeyStringFLOAT64(n.(float64))
	case Date:
		return ToKeyStringDATE(n.(Date))
	case Timestamp:
		return ToKeyStringTIMESTAMP(n.(Timestamp))
	}
	return fmt.Sprintf("%v", n)
}
