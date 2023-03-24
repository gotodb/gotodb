package config

type Runtime struct {
	Catalog                 string
	Schema                  string
	Table                   string
	Priority                int32
	ParallelNumber          int32
	MaxConcurrentTaskNumber int32
	MaxQueueSize            int32
}

func NewConfigRuntime() *Runtime {
	return &Runtime{
		Catalog:                 "default",
		Schema:                  "default",
		Table:                   "default",
		Priority:                0,
		ParallelNumber:          4,
		MaxConcurrentTaskNumber: 2,
		MaxQueueSize:            100,
	}
}
