package gtype

import (
	"time"
)

type Date struct {
	Sec int64
}

func (d Date) String() string {
	return time.Unix(d.Sec, 0).Format("2006-01-02")
}
