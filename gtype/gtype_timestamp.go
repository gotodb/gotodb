package gtype

import (
	"time"
)

type Timestamp struct {
	Sec int64
}

func (ts Timestamp) String() string {
	return time.Unix(ts.Sec, 0).Format("2006-01-02 15:04:05")
}
