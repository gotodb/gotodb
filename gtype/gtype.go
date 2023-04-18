package gtype

import (
	"fmt"
	"strings"
	"time"
)

type OrderType int32

const (
	UnknownOrderType OrderType = iota
	ASC
	DESC
	FIRST
	LAST
)

type FuncType int32

const (
	UnknownFuncType FuncType = iota
	AGGREGATE
	NORMAL
)

type QuantifierType int32

const (
	UnknownQuantifierType QuantifierType = iota
	ALL
	DISTINCT
	SOME
	ANY
)

func StrToQuantifierType(s string) QuantifierType {
	switch strings.ToUpper(s) {
	case "ALL":
		return ALL
	case "DISTINCT":
		return DISTINCT
	case "SOME":
		return SOME
	case "ANY":
		return ANY
	default:
		return UnknownQuantifierType
	}
}

func CheckType(ta, tb Type, op Operator) (Type, error) {
	if ta != tb || ta == UNKNOWNTYPE {
		return UNKNOWNTYPE, fmt.Errorf("type not match")
	}
	return ta, nil
}

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
