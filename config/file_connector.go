package config

import (
	"fmt"
	"strings"
)

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

func (c FileConnectors) Check() error {
	for pattern, conf := range c {
		ns := strings.Split(pattern, ".")
		if len(ns) < 3 {
			return fmt.Errorf("file config name error: %s", pattern)
		}
		if len(conf.ColumnNames) != len(conf.ColumnTypes) {
			return fmt.Errorf("column names (%d) doesn't match column types (%d)", len(conf.ColumnNames), len(conf.ColumnTypes))
		}
	}
	return nil
}
