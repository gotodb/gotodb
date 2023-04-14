package connector

import (
	"database/sql"
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"
	"io"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Mysql struct {
	Config        *config.MysqlConnector
	Metadata      *metadata.Metadata
	PartitionInfo *partition.Info
}

func NewMysqlConnectorEmpty() *Mysql {
	return &Mysql{}
}

func NewMysqlConnector(catalog, schema, table string) (*Mysql, error) {
	var err error
	res := &Mysql{}
	key := strings.Join([]string{catalog, schema, table}, ".")
	conf := config.Conf.MysqlConnectors.GetConfig(key)
	if conf == nil {
		return nil, fmt.Errorf("mysql connector: table not found")
	}
	res.Config = conf
	res.Metadata, err = NewMysqlMetadata(conf)

	return res, err
}

func NewMysqlMetadata(conf *config.MysqlConnector) (*metadata.Metadata, error) {
	res := metadata.NewMetadata()
	for i := 0; i < len(conf.ColumnNames); i++ {
		col := &metadata.ColumnMetadata{
			Catalog:    conf.Catalog,
			Schema:     conf.Schema,
			Table:      conf.Table,
			ColumnName: conf.ColumnNames[i],
			ColumnType: gtype.NameToType(conf.ColumnTypes[i]),
		}
		res.AppendColumn(col)
	}

	res.Reset()
	return res, nil
}

func (c *Mysql) GetMetadata() (*metadata.Metadata, error) {
	return c.Metadata, nil
}

func (c *Mysql) GetPartitionInfo(partitionNumber int) (*partition.Info, error) {
	if c.PartitionInfo == nil {
		c.PartitionInfo = partition.New(metadata.NewMetadata())
		for i := 0; i < partitionNumber; i++ {
			c.PartitionInfo.Locations = append(c.PartitionInfo.Locations, fmt.Sprintf("%d/%d", i, partitionNumber))
		}
	}
	return c.PartitionInfo, nil
}

func (c *Mysql) GetReader(file *filesystem.FileLocation, md *metadata.Metadata, filters []*operator.BooleanExpressionNode) (IndexReader, error) {
	alias := ""
	if c.Config.Table != md.Columns[0].Table {
		alias = md.Columns[0].Table + "."
	}

	selectItems := ""
	for _, column := range md.Columns {
		selectItems += alias + column.ColumnName + ","
	}
	selectItems = strings.Trim(selectItems, ",")

	var clauses []string
	for _, filter := range filters {
		clauses = append(clauses, "("+filter.Clause+")")
	}
	clause := strings.Join(clauses, " AND ")

	var stop error
	var part, partitionNumber int
	_, _ = fmt.Sscanf(file.Location, "%d/%d", &part, &partitionNumber)
	return func(indexes []int) (*row.RowsGroup, error) {
		if part > 0 {
			return nil, io.EOF
		}

		if stop != nil {
			return nil, stop
		}

		dsn := c.getDSN()
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		rows, err := db.Query(fmt.Sprintf("select %s from %s.%s %s where %s", selectItems, c.Config.Schema, c.Config.Table, strings.TrimRight(alias, "."), clause))
		if err != nil {
			return nil, err
		}

		record := make([]interface{}, len(md.Columns))
		for i := range md.Columns {
			record[i] = new(interface{})
		}
		rg := row.NewRowsGroup(md)
		for rows.Next() {
			if err := rows.Scan(record...); err != nil {
				return nil, err
			}
			for i, column := range md.Columns {
				index := md.ColumnMap[column.ColumnName]
				b := *record[i].(*interface{})
				rg.Vals[index] = append(rg.Vals[index], gtype.BytesToType(b.([]byte), column.ColumnType))
			}
			rg.RowsNumber++
		}
		stop = io.EOF
		return rg, nil
	}, nil
}

func (c *Mysql) ShowSchemas(catalog string, _, _ *string) RowReader {
	var err error
	var rs []*row.Row
	for key := range config.Conf.MysqlConnectors {
		ns := strings.Split(key, ".")
		c, s, _ := ns[0], ns[1], ns[2]
		if c == catalog {
			r := row.NewRow()
			r.AppendVals(s)
			rs = append(rs, r)
			break
		}
	}
	i := 0

	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		if i >= len(rs) {
			return nil, io.EOF
		}
		i++
		return rs[i-1], nil
	}
}

func (c *Mysql) ShowTables(catalog, schema string, _, _ *string) RowReader {
	var err error
	var rs []*row.Row
	for key := range config.Conf.MysqlConnectors {
		ns := strings.Split(key, ".")
		c, s, t := ns[0], ns[1], ns[2]
		if c == catalog && s == schema {
			r := row.NewRow()
			r.AppendVals(t)
			rs = append(rs, r)
		}
	}

	i := 0
	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		if i >= len(rs) {
			return nil, io.EOF
		}
		i++
		return rs[i-1], nil
	}
}

func (c *Mysql) ShowColumns(catalog, schema, table string) RowReader {
	var err error
	var rs []*row.Row
	for key, conf := range config.Conf.MysqlConnectors {
		ns := strings.Split(key, ".")
		c, s, t := ns[0], ns[1], ns[2]
		if c == catalog && s == schema && t == table {
			for i, name := range conf.ColumnNames {
				r := row.NewRow()
				r.AppendVals(name, conf.ColumnTypes[i])
				rs = append(rs, r)
			}
			break
		}
	}

	i := 0
	return func() (*row.Row, error) {
		if err != nil {
			return nil, err
		}
		if i >= len(rs) {
			return nil, io.EOF
		}

		i++
		return rs[i-1], nil
	}
}

func (c *Mysql) ShowPartitions(_, _, _ string) RowReader {
	return func() (*row.Row, error) {
		return nil, io.EOF
	}
}

func (c *Mysql) getDSN() string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", c.Config.User, c.Config.Password, c.Config.Host, c.Config.Port)
	return dsn
}
