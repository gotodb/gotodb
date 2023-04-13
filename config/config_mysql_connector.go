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

func (c MysqlConnectors) GetConfig(name string) *MysqlConnector {
	for pattern, config := range c {
		if name == pattern {
			return config
		}
	}
	return nil
}

func (c MysqlConnectors) Check() error {
	// DSN:Data Source Name
	dsn := "user:password@tcp(127.0.0.1:3306)/dbname"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for pattern, conf := range c {
		ns := strings.Split(pattern, ".")
		if len(ns) != 3 {
			return fmt.Errorf("mysql config name error: %s", pattern)
		}

		if len(conf.ColumnNames) != len(conf.ColumnTypes) {
			return fmt.Errorf("column names (%d) doesn't match column types (%d)", len(conf.ColumnNames), len(conf.ColumnTypes))
		}

		if ns[1] != "*" && ns[2] != "*" && len(conf.ColumnNames) == 0 {
			return fmt.Errorf("column names (%d) must be set", len(conf.ColumnNames))
		}

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", conf.User, conf.Password, conf.Host, conf.Port)
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

				descs, err := db.Query(fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", dbname, tableName))
				if err != nil {
					panic(err)
				}

				var desc = struct {
					Field      string
					DataType   string
					ColumnType string
				}{}

				temp := *conf
				temp.ColumnNames = []string{}
				temp.ColumnTypes = []string{}
				for descs.Next() {
					if err := descs.Scan(&desc.Field, &desc.DataType, &desc.ColumnType); err != nil {
						log.Fatal(err)
					}
					temp.ColumnNames = append(temp.ColumnNames, desc.Field)
					temp.ColumnTypes = append(temp.ColumnTypes, gtype.ConvertMysqlType(desc.DataType, strings.Contains(desc.ColumnType, "unsigned")).String())

				}
				c[fmt.Sprintf("%s.%s.%s", ns[0], dbname, tableName)] = &temp
			}

		}
		_ = db.Close()
		fmt.Printf("%v\n", c["mysql.goploy.user"])
	}
	return nil
}
