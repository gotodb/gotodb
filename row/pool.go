package row

import (
	"sync"
)

var Pool *sync.Pool

func init() {
	Pool = &sync.Pool{
		New: func() interface{} {
			return NewRow()
		},
	}
}
