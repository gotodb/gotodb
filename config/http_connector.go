package config

import (
	"fmt"
	"strings"
)

type HttpConnector struct {
	Catalog      string   `yaml:"catalog"`
	Schema       string   `yaml:"schema"`
	Table        string   `yaml:"table"`
	DataPath     string   `yaml:"data-path"`
	FilterColumn string   `yaml:"filter-column"`
	ResultColumn string   `yaml:"result-column"`
	ColumnNames  []string `yaml:"column-names"`
	ColumnTypes  []string `yaml:"column-types"`
}
type HttpConnectors map[string]*HttpConnector

func (c HttpConnectors) GetConfig(name string) *HttpConnector {
	for pattern, config := range c {
		if WildcardMatch(name, pattern) {
			return config
		}
	}
	return nil
}

func (c HttpConnectors) Check() error {
	for pattern, conf := range c {
		ns := strings.Split(pattern, ".")
		if len(ns) < 3 {
			return fmt.Errorf("http config name error: %s", pattern)
		}
		if len(conf.ColumnNames) != len(conf.ColumnTypes) {
			return fmt.Errorf("column names (%d) doesn't match column types (%d)", len(conf.ColumnNames), len(conf.ColumnTypes))
		}
	}
	return nil
}
