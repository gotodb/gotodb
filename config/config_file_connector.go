package config

type FileConnector struct {
	Catalog     string   `yaml:"catalog"`
	Schema      string   `yaml:"schema"`
	Table       string   `yaml:"table"`
	FileType    string   `yaml:"file-type"`
	ColumnNames []string `yaml:"column-names"`
	ColumnTypes []string `yaml:"column-types"`
	Paths       []string `yaml:"paths"`
}
type FileConnectors map[string]*FileConnector

func (c FileConnectors) GetConfig(name string) *FileConnector {
	for pattern, config := range c {
		if WildcardMatch(name, pattern) {
			return config
		}
	}
	return nil
}
