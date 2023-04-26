package datatype

import (
	"strings"
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

func ToQuantifierType(s string) QuantifierType {
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
