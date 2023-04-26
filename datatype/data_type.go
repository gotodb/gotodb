package datatype

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

type Type int32

const (
	UnknownType Type = iota
	STRING
	FLOAT64
	FLOAT32
	INT8
	UINT8
	INT16
	UINT16
	INT32
	UINT32
	INT64
	UINT64
	BOOL
	TIMESTAMP
	DATE
)

type Date struct {
	Sec int64
}

func (d Date) String() string {
	return time.Unix(d.Sec, 0).Format("2006-01-02")
}

type Timestamp struct {
	Sec int64
}

func (ts Timestamp) String() string {
	return time.Unix(ts.Sec, 0).Format("2006-01-02 15:04:05")
}

func (t Type) String() string {
	switch t {
	case STRING:
		return "STRING"
	case FLOAT32:
		return "FLOAT32"
	case FLOAT64:
		return "FLOAT64"
	case INT8:
		return "INT8"
	case INT16:
		return "INT16"
	case INT32:
		return "INT32"
	case INT64:
		return "INT64"
	case UINT8:
		return "UINT8"
	case UINT16:
		return "UINT16"
	case UINT32:
		return "UINT32"
	case UINT64:
		return "UINT64"
	case BOOL:
		return "BOOL"
	case TIMESTAMP:
		return "TIMESTAMP"
	case DATE:
		return "DATE"
	}
	return "UnknownType"
}

func FromMysql(name string, unsigned bool) Type {
	switch name {
	case "varchar", "decimal", "text":
		return STRING
	case "tinyint":
		if unsigned {
			return UINT8
		} else {
			return INT8
		}
	case "smallint":
		if unsigned {
			return UINT16
		} else {
			return INT16
		}
	case "mediumint", "int", "integer":
		if unsigned {
			return UINT32
		} else {
			return INT32
		}
	case "bigint":
		if unsigned {
			return UINT64
		} else {
			return INT32
		}
	case "float":
		return FLOAT32
	case "double":
		return FLOAT64
	case "datetime":
		return STRING
	default:
		return UnknownType
	}
}

func FromString(name string) Type {
	switch name {
	case "STRING":
		return STRING
	case "FLOAT32":
		return FLOAT32
	case "FLOAT64":
		return FLOAT64
	case "INT8":
		return INT8
	case "INT16":
		return INT16
	case "INT32":
		return INT32
	case "INT64":
		return INT64
	case "UINT8":
		return UINT8
	case "UINT16":
		return UINT16
	case "UINT32":
		return UINT32
	case "UINT64":
		return UINT64
	case "BOOL":
		return BOOL
	case "TIMESTAMP":
		return TIMESTAMP
	case "DATE":
		return DATE
	default:
		return UnknownType
	}
}

func TypeOf(v any) Type {
	switch v.(type) {
	case bool:
		return BOOL
	case int8:
		return INT8
	case int16:
		return INT16
	case int32:
		return INT32
	case int64:
		return INT64
	case uint8:
		return UINT8
	case uint16:
		return UINT16
	case uint32:
		return UINT32
	case uint64:
		return UINT64
	case float32:
		return FLOAT32
	case float64:
		return FLOAT64
	case string:
		return STRING
	case Timestamp:
		return TIMESTAMP
	case Date:
		return DATE
	default:
		return UnknownType
	}
}

func ToString(iv any) string {
	switch v := iv.(type) {
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}

}

func ToInt8(iv any) int8 {
	var res int8
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = v
	case int16:
		res = int8(v)
	case int32:
		res = int8(v)
	case int64:
		res = int8(v)
	case uint8:
		res = int8(v)
	case uint16:
		res = int8(v)
	case uint32:
		res = int8(v)
	case uint64:
		res = int8(v)
	case float32:
		res = int8(v)
	case float64:
		res = int8(v)
	case string:
		tmp, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			res = int8(tmp)
		}
	case Timestamp:
		res = int8(v.Sec)
	case Date:
		res = int8(v.Sec)
	case []byte:
		d, err := strconv.ParseInt(string(v), 10, 8)
		if err == nil {
			res = int8(d)
		}
	}
	return res
}

func ToInt16(iv any) int16 {
	var res int16
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = int16(v)
	case int16:
		res = v
	case int32:
		res = int16(v)
	case int64:
		res = int16(v)
	case uint8:
		res = int16(v)
	case uint16:
		res = int16(v)
	case uint32:
		res = int16(v)
	case uint64:
		res = int16(v)
	case float32:
		res = int16(v)
	case float64:
		res = int16(v)
	case string:
		tmp, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			res = int16(tmp)
		}
	case Timestamp:
		res = int16(v.Sec)
	case Date:
		res = int16(v.Sec)
	case []byte:
		d, err := strconv.ParseInt(string(v), 10, 16)
		if err == nil {
			res = int16(d)
		}
	}
	return res
}

func ToInt32(iv any) int32 {
	var res int32
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = int32(v)
	case int16:
		res = int32(v)
	case int32:
		res = v
	case int64:
		res = int32(v)
	case uint8:
		res = int32(v)
	case uint16:
		res = int32(v)
	case uint32:
		res = int32(v)
	case uint64:
		res = int32(v)
	case float32:
		res = int32(v)
	case float64:
		res = int32(v)
	case string:
		tmp, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			res = int32(tmp)
		}
	case Timestamp:
		res = int32(v.Sec)
	case Date:
		res = int32(v.Sec)
	case []byte:
		d, err := strconv.ParseInt(string(v), 10, 32)
		if err == nil {
			res = int32(d)
		}
	}
	return res
}

func ToInt64(iv any) int64 {
	var res int64
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = int64(v)
	case int16:
		res = int64(v)
	case int32:
		res = int64(v)
	case int64:
		res = v
	case uint8:
		res = int64(v)
	case uint16:
		res = int64(v)
	case uint32:
		res = int64(v)
	case uint64:
		res = int64(v)
	case float32:
		res = int64(v)
	case float64:
		res = int64(v)
	case string:
		tmp, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			res = tmp
		}
	case time.Time:
		res = v.Unix()
	case Date:
		res = v.Sec
	case []byte:
		d, err := strconv.ParseInt(string(v), 10, 64)
		if err == nil {
			res = d
		}
	}
	return res
}

func ToUint8(iv any) uint8 {
	var res uint8
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = uint8(v)
	case int16:
		res = uint8(v)
	case int32:
		res = uint8(v)
	case int64:
		res = uint8(v)
	case uint8:
		res = v
	case uint16:
		res = uint8(v)
	case uint32:
		res = uint8(v)
	case uint64:
		res = uint8(v)
	case float32:
		res = uint8(v)
	case float64:
		res = uint8(v)
	case string:
		tmp, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			res = uint8(tmp)
		}
	case Timestamp:
		res = uint8(v.Sec)
	case Date:
		res = uint8(v.Sec)
	case []byte:
		d, err := strconv.ParseUint(string(v), 10, 8)
		if err == nil {
			res = uint8(d)
		}
	}
	return res
}

func ToUint16(iv any) uint16 {
	var res uint16
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = uint16(v)
	case int16:
		res = uint16(v)
	case int32:
		res = uint16(v)
	case int64:
		res = uint16(v)
	case uint8:
		res = uint16(v)
	case uint16:
		res = v
	case uint32:
		res = uint16(v)
	case uint64:
		res = uint16(v)
	case float32:
		res = uint16(v)
	case float64:
		res = uint16(v)
	case string:
		tmp, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			res = uint16(tmp)
		}
	case Timestamp:
		res = uint16(v.Sec)
	case Date:
		res = uint16(v.Sec)
	case []byte:
		d, err := strconv.ParseUint(string(v), 10, 16)
		if err == nil {
			res = uint16(d)
		}
	}
	return res
}

func ToUint32(iv any) uint32 {
	var res uint32
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = uint32(v)
	case int16:
		res = uint32(v)
	case int32:
		res = uint32(v)
	case int64:
		res = uint32(v)
	case uint8:
		res = uint32(v)
	case uint16:
		res = uint32(v)
	case uint32:
		res = v
	case uint64:
		res = uint32(v)
	case float32:
		res = uint32(v)
	case float64:
		res = uint32(v)
	case string:
		tmp, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			res = uint32(tmp)
		}
	case Timestamp:
		res = uint32(v.Sec)
	case Date:
		res = uint32(v.Sec)
	case []byte:
		d, err := strconv.ParseUint(string(v), 10, 32)
		if err == nil {
			res = uint32(d)
		}
	}
	return res
}

func ToUint64(iv any) uint64 {
	var res uint64
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1
		}
	case int8:
		res = uint64(v)
	case int16:
		res = uint64(v)
	case int32:
		res = uint64(v)
	case int64:
		res = uint64(v)
	case uint8:
		res = uint64(v)
	case uint16:
		res = uint64(v)
	case uint32:
		res = uint64(v)
	case uint64:
		res = v
	case float32:
		res = uint64(v)
	case float64:
		res = uint64(v)
	case string:
		tmp, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			res = 0
		} else {
			res = tmp
		}
	case time.Time:
		res = uint64(v.Unix())
	case Date:
		res = uint64(v.Sec)
	case []byte:
		d, err := strconv.ParseUint(string(v), 10, 64)
		if err == nil {
			res = d
		}
	}
	return res
}

func ToFloat32(iv any) float32 {
	var res float32
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1.0
		}
	case int8:
		res = float32(v)
	case int16:
		res = float32(v)
	case int32:
		res = float32(v)
	case int64:
		res = float32(v)
	case uint8:
		res = float32(v)
	case uint16:
		res = float32(v)
	case uint32:
		res = float32(v)
	case uint64:
		res = float32(v)
	case float32:
		res = v
	case float64:
		res = float32(v)
	case string:
		tmp, err := strconv.ParseFloat(v, 32)
		if err == nil {
			res = float32(tmp)
		}
	case time.Time:
		res = float32(v.Unix())
	case Date:
		res = float32(v.Sec)
	case []byte:
		d, err := strconv.ParseFloat(string(v), 32)
		if err == nil {
			res = float32(d)
		}
	}
	return res
}

func ToFloat64(iv any) float64 {
	var res float64
	switch v := iv.(type) {
	case bool:
		if v {
			res = 1.0
		}
	case int8:
		res = float64(v)
	case int16:
		res = float64(v)
	case int32:
		res = float64(v)
	case int64:
		res = float64(v)
	case uint8:
		res = float64(v)
	case uint16:
		res = float64(v)
	case uint32:
		res = float64(v)
	case uint64:
		res = float64(v)
	case float32:
		res = float64(v)
	case float64:
		res = v
	case string:
		tmp, err := strconv.ParseFloat(v, 64)
		if err == nil {
			res = tmp
		}
	case time.Time:
		res = float64(v.Unix())
	case Date:
		res = float64(v.Sec)
	case []byte:
		d, err := strconv.ParseFloat(string(v), 64)
		if err == nil {
			res = d
		}
	}
	return res
}

func ToTimestamp(iv any) Timestamp {
	var res Timestamp
	var err error
	var sec int64
	switch v := iv.(type) {
	case bool:
	case int8:
		sec = int64(v)
	case int16:
		sec = int64(v)
	case int32:
		sec = int64(v)
	case int64:
		sec = v
	case uint8:
		sec = int64(v)
	case uint16:
		sec = int64(v)
	case uint32:
		sec = int64(v)
	case uint64:
		sec = int64(v)
	case float32:
		sec = int64(v)
	case float64:
		sec = int64(v)
	case string:
		var t time.Time
		if t, err = time.Parse(time.RFC3339, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.UnixDate, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RubyDate, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC822, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC822Z, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC850, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC1123, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC1123Z, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC3339, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC3339Nano, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse("2006-01-02", v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse("2006-01-02 15:04:05", v); err == nil {
			sec = t.Unix()
		}

	case Timestamp:
		sec = v.Sec
	case Date:
		sec = v.Sec
	case []byte:
		d, err := strconv.ParseInt(string(v), 10, 64)
		if err == nil {
			res = Timestamp{Sec: d}
		}
	}
	res.Sec = sec
	return res
}

func ToDate(iv any) Date {
	var res Date
	var err error
	var sec int64
	switch v := iv.(type) {
	case bool:
	case int8:
		sec = int64(v)
	case int16:
		sec = int64(v)
	case int32:
		sec = int64(v)
	case int64:
		sec = v
	case uint8:
		sec = int64(v)
	case uint16:
		sec = int64(v)
	case uint32:
		sec = int64(v)
	case uint64:
		sec = int64(v)
	case float32:
		sec = int64(v)
	case float64:
		sec = int64(v)
	case string:
		var t time.Time
		if t, err = time.Parse(time.RFC3339, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.UnixDate, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RubyDate, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC822, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC822Z, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC850, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC1123, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC1123Z, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC3339, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse(time.RFC3339Nano, v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse("2006-01-02", v); err == nil {
			sec = t.Unix()
		} else if t, err = time.Parse("2006-01-02 15:04:05", v); err == nil {
			sec = t.Unix()
		}

	case Timestamp:
		sec = v.Sec
	case Date:
		sec = v.Sec
	case []byte:
		d, err := strconv.ParseInt(string(v), 10, 64)
		if err == nil {
			res = Date{Sec: d}
		}
	}
	res.Sec = sec
	return res
}

func ToBool(iv any) bool {
	var res bool
	switch v := iv.(type) {
	case bool:
		res = v
	case int8:
		if v != 0 {
			res = true
		}
	case int16:
		if v != 0 {
			res = true
		}
	case int32:
		if v != 0 {
			res = true
		}
	case int64:
		if v != 0 {
			res = true
		}
	case uint8:
		if v != 0 {
			res = true
		}
	case uint16:
		if v != 0 {
			res = true
		}
	case uint32:
		if v != 0 {
			res = true
		}
	case uint64:
		if v != 0 {
			res = true
		}
	case float32:
		if v != 0 {
			res = true
		}
	case float64:
		if v != 0 {
			res = true
		}
	case string:
		if v != "" {
			res = true
		}
	case time.Time:
		res = true
	case Date:
		res = true
	case []byte:
		res = v[0] > 0
	}
	return res
}

func ToBytes(iv any) []byte {
	var res []byte
	switch v := iv.(type) {
	case bool:
		res = make([]byte, 1)
		if v {
			res[0] = byte(1)
		} else {
			res[0] = byte(0)
		}
	case int8:
		res = make([]byte, 1)
		res[0] = byte(v)
	case int16:
		res = make([]byte, 2)
		res[0] = byte(v)
		res[1] = byte(v >> 8)
	case int32:
		res = make([]byte, 4)
		res[0] = byte(v)
		res[1] = byte(v >> 8)
		res[2] = byte(v >> 16)
		res[3] = byte(v >> 24)
	case int64:
		res = make([]byte, 8)
		res[0] = byte(v)
		res[1] = byte(v >> 8)
		res[2] = byte(v >> 16)
		res[3] = byte(v >> 24)
		res[4] = byte(v >> 32)
		res[5] = byte(v >> 40)
		res[6] = byte(v >> 48)
		res[7] = byte(v >> 56)
	case uint8:
		res = make([]byte, 1)
		res[0] = v
	case uint16:
		res = make([]byte, 2)
		res[0] = byte(v)
		res[1] = byte(v >> 8)
	case uint32:
		res = make([]byte, 4)
		res[0] = byte(v)
		res[1] = byte(v >> 8)
		res[2] = byte(v >> 16)
		res[3] = byte(v >> 24)
	case uint64:
		res = make([]byte, 8)
		res[0] = byte(v)
		res[1] = byte(v >> 8)
		res[2] = byte(v >> 16)
		res[3] = byte(v >> 24)
		res[4] = byte(v >> 32)
		res[5] = byte(v >> 40)
		res[6] = byte(v >> 48)
		res[7] = byte(v >> 56)
	case float32:
		n := math.Float32bits(v)
		res = make([]byte, 4)
		res[0] = byte(n)
		res[1] = byte(n >> 8)
		res[2] = byte(n >> 16)
		res[3] = byte(n >> 24)
	case float64:
		n := math.Float64bits(v)
		res = make([]byte, 4)
		res[0] = byte(n)
		res[1] = byte(n >> 8)
		res[2] = byte(n >> 16)
		res[3] = byte(n >> 24)
		res[4] = byte(n >> 32)
		res[5] = byte(n >> 40)
		res[6] = byte(n >> 48)
		res[7] = byte(n >> 56)
	case string:
		res = []byte(v)
	case time.Time:
		sec := v.Unix()
		res = make([]byte, 8)
		res[0] = byte(sec)
		res[1] = byte(sec >> 8)
		res[2] = byte(sec >> 16)
		res[3] = byte(sec >> 24)
		res[4] = byte(sec >> 32)
		res[5] = byte(sec >> 40)
		res[6] = byte(sec >> 48)
		res[7] = byte(sec >> 56)
	case Date:
		sec := v.Sec
		res = make([]byte, 8)
		res[0] = byte(sec)
		res[1] = byte(sec >> 8)
		res[2] = byte(sec >> 16)
		res[3] = byte(sec >> 24)
		res[4] = byte(sec >> 32)
		res[5] = byte(sec >> 40)
		res[6] = byte(sec >> 48)
		res[7] = byte(sec >> 56)
	}
	return res
}

func ToValue(v any, t Type) any {
	var res any
	switch t {
	case BOOL:
		res = ToBool(v)
	case INT8:
		res = ToInt8(v)
	case INT16:
		res = ToInt16(v)
	case INT32:
		res = ToInt32(v)
	case INT64:
		res = ToInt64(v)
	case UINT8:
		res = ToUint8(v)
	case UINT16:
		res = ToUint16(v)
	case UINT32:
		res = ToUint32(v)
	case UINT64:
		res = ToUint64(v)
	case FLOAT32:
		res = ToFloat32(v)
	case FLOAT64:
		res = ToFloat64(v)
	case STRING:
		res = ToString(v)
	case TIMESTAMP:
		res = ToTimestamp(v)
	case DATE:
		res = ToDate(v)
	}
	return res
}

func ToSameTypeValue(va any, vb any) (any, any) {
	ta, tb := TypeOf(va), TypeOf(vb)
	var t Type
	if tb >= ta {
		t = ta
	} else {
		t = tb
	}
	return ToValue(va, t), ToValue(vb, t)
}
