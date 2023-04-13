package config

import (
	"database/sql"
	"fmt"
	"github.com/gotodb/gotodb/gtype"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlConnector struct {
	Catalog     string   `yaml:"catalog"`
	Schema      string   `yaml:"schema"`
	Table       string   `yaml:"table"`
	Host        string   `yaml:"host"`
	Port        string   `yaml:"port"`
	User        string   `yaml:"user"`
	Password    string   `yaml:"password"`
	ColumnNames []string `yaml:"column-names"`
	ColumnTypes []string `yaml:"column-types"`
}
type MysqlConnectors map[string]*MysqlConnector

func (m MysqlConnectors) GetConfig(name string) *MysqlConnector {
	for pattern, config := range m {
		if name == pattern {
			return config
		}
	}
	return nil
}

func (m MysqlConnectors) Check() error {
	for pattern, c := range m {
		ns := strings.Split(pattern, ".")
		if len(ns) != 3 {
			return fmt.Errorf("mysql config name error: %s", pattern)
		}

		if len(c.ColumnNames) != len(c.ColumnTypes) {
			return fmt.Errorf("column names (%d) doesn't match column types (%d)", len(c.ColumnNames), len(c.ColumnTypes))
		}

		if ns[1] != "*" && ns[2] != "*" && len(c.ColumnNames) == 0 {
			return fmt.Errorf("column names (%d) must be set", len(c.ColumnNames))
		}

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", c.User, c.Password, c.Host, c.Port)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			panic(err)
		}

		databases, err := db.Query("show databases")
		if err != nil {
			panic(err)
		}
		var dbname string
		for databases.Next() {
			err := databases.Scan(&dbname)
			if err != nil {
				log.Fatal(err)
			}
			if ns[1] != "*" && dbname != ns[1] {
				continue
			}

			tables, err := db.Query("show tables from " + dbname)
			if err != nil {
				panic(err)
			}

			var tableName string
			for tables.Next() {
				if err := tables.Scan(&tableName); err != nil {
					log.Fatal(err)
				}
				if ns[2] != "*" && tableName != ns[2] {
					continue
				}

				columns, err := db.Query(fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", dbname, tableName))
				if err != nil {
					log.Fatal(err)
				}

				var column = struct {
					Field      string
					DataType   string
					ColumnType string
				}{}

				temp := *c
				temp.ColumnNames = []string{}
				temp.ColumnTypes = []string{}
				for columns.Next() {
					if err := columns.Scan(&column.Field, &column.DataType, &column.ColumnType); err != nil {
						log.Fatal(err)
					}
					temp.ColumnNames = append(temp.ColumnNames, column.Field)
					temp.ColumnTypes = append(temp.ColumnTypes, gtype.ConvertMysqlType(column.DataType, strings.Contains(column.ColumnType, "unsigned")).String())

				}
				m[fmt.Sprintf("%s.%s.%s", ns[0], dbname, tableName)] = &temp
			}
		}
		_ = db.Close()
	}
	return nil
}
