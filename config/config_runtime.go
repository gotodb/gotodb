package config

type Runtime struct {
	Catalog        string `yaml:"catalog"`
	Schema         string `yaml:"schema"`
	Table          string `yaml:"table"`
	ParallelNumber int    `yaml:"parallel-number"`
}

func NewConfigRuntime() *Runtime {
	return &Runtime{
		Catalog:        "default",
		Schema:         "default",
		ParallelNumber: 4,
	}
}
