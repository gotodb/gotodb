package gtype

import (
	"fmt"
	"strconv"
	"time"
)

type Type int32

const (
	UNKNOWNTYPE Type = iota
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
	return "UNKNOWNTYPE"
}

func ConvertMysqlType(name string, unsigned bool) Type {
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
		return UNKNOWNTYPE
	}
}

func NameToType(name string) Type {
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
		return UNKNOWNTYPE
	}
}

func TypeOf(v interface{}) Type {
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
		return UNKNOWNTYPE
	}
}

func ToString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func ToInt8(iv interface{}) int8 {
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
		if err != nil {
			res = 0
		} else {
			res = int8(tmp)
		}
	case Timestamp:
		res = int8(v.Sec)
	case Date:
		res = int8(v.Sec)
	}
	return res
}

func ToInt16(iv interface{}) int16 {
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
		if err != nil {
			res = 0
		} else {
			res = int16(tmp)
		}
	case Timestamp:
		res = int16(v.Sec)
	case Date:
		res = int16(v.Sec)
	}
	return res
}

func ToInt32(iv interface{}) int32 {
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
		if err != nil {
			res = 0
		} else {
			res = int32(tmp)
		}
	case Timestamp:
		res = int32(v.Sec)
	case Date:
		res = int32(v.Sec)
	}
	return res
}

func ToInt64(iv interface{}) int64 {
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
		if err != nil {
			res = 0
		} else {
			res = tmp
		}
	case time.Time:
		res = v.Unix()
	case Date:
		res = v.Sec
	}
	return res
}

func ToUint8(iv interface{}) uint8 {
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
		if err != nil {
			res = 0
		} else {
			res = uint8(tmp)
		}
	case Timestamp:
		res = uint8(v.Sec)
	case Date:
		res = uint8(v.Sec)
	}
	return res
}

func ToUint16(iv interface{}) uint16 {
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
		if err != nil {
			res = 0
		} else {
			res = uint16(tmp)
		}
	case Timestamp:
		res = uint16(v.Sec)
	case Date:
		res = uint16(v.Sec)
	}
	return res
}

func ToUint32(iv interface{}) uint32 {
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
		if err != nil {
			res = 0
		} else {
			res = uint32(tmp)
		}
	case Timestamp:
		res = uint32(v.Sec)
	case Date:
		res = uint32(v.Sec)
	}
	return res
}

func ToUint64(iv interface{}) uint64 {
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
	}
	return res
}

func ToFloat32(iv interface{}) float32 {
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
		if err != nil {
			res = 0
		} else {
			res = float32(tmp)
		}
	case time.Time:
		res = float32(v.Unix())
	case Date:
		res = float32(v.Sec)
	}
	return res
}

func ToFloat64(iv interface{}) float64 {
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
		if err != nil {
			res = 0
		} else {
			res = tmp
		}
	case time.Time:
		res = float64(v.Unix())
	case Date:
		res = float64(v.Sec)
	}
	return res
}

func ToTimestamp(iv interface{}) Timestamp {
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
	}
	res.Sec = sec
	return res
}

func ToDate(iv interface{}) Date {
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
	}
	res.Sec = sec
	return res
}

func ToBool(iv interface{}) bool {
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
	}
	return res
}

func ToType(v interface{}, t Type) interface{} {
	var res interface{}
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

func BytesToType(b []byte, t Type) interface{} {
	var res interface{}
	s := string(b)
	switch t {
	case BOOL:
		res = b[0] > 0
	case INT8:
		v, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			res = nil
		}
		res = int8(v)
	case INT16:
		v, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			res = nil
		}
		res = int16(v)
	case INT32:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			res = nil
		}
		res = int32(v)
	case INT64:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			res = nil
		}
		res = v
	case UINT8:
		v, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			res = nil
		}
		res = uint8(v)
	case UINT16:
		v, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			res = nil
		}
		res = uint16(v)
	case UINT32:
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			res = nil
		}
		res = uint32(v)
	case UINT64:
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			res = nil
		}
		res = v
	case FLOAT32:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			res = nil
		}
		res = v
	case FLOAT64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			res = nil
		}
		res = v
	case STRING:
		res = string(b)
	case TIMESTAMP:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			res = nil
		}
		res = Timestamp{Sec: v}
	case DATE:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			res = nil
		}
		res = Date{Sec: v}
	}

	return res
}

func ToSameType(va interface{}, vb interface{}) (interface{}, interface{}) {
	ta, tb := TypeOf(va), TypeOf(vb)
	var t Type
	if tb >= ta {
		t = ta
	} else {
		t = tb
	}
	return ToType(va, t), ToType(vb, t)
}
