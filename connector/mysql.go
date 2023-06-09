package connector

import (
	"database/sql"
	"fmt"
	"github.com/gotodb/gotodb/partition"
	"io"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Mysql struct {
	Config    *config.MysqlConnector
	Metadata  *metadata.Metadata
	Partition *partition.Partition
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
			ColumnType: datatype.FromString(conf.ColumnTypes[i]),
		}
		res.AppendColumn(col)
	}

	res.Reset()
	return res, nil
}

func (c *Mysql) GetMetadata() (*metadata.Metadata, error) {
	return c.Metadata, nil
}

func (c *Mysql) GetPartition(partitionNumber int) (*partition.Partition, error) {
	if c.Partition == nil {
		c.Partition = partition.New(metadata.NewMetadata())
		c.Partition.Locations = append(c.Partition.Locations, fmt.Sprintf("%d/%d", 0, partitionNumber))

	}
	return c.Partition, nil
}

func (c *Mysql) GetReader(file *partition.FileLocation, md *metadata.Metadata, filters []string) (row.GroupReader, error) {
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
		clauses = append(clauses, "("+filter+")")
	}
	clause := strings.Join(clauses, " AND ")

	if clause != "" {
		clause = " where " + clause
	}

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
		rows, err := db.Query(fmt.Sprintf("select %s from %s.%s %s %s", selectItems, c.Config.Schema, c.Config.Table, strings.TrimRight(alias, "."), clause))
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
				rg.Vals[index] = append(rg.Vals[index], datatype.ToValue(b.([]byte), column.ColumnType))
			}
			rg.RowsNumber++
		}
		stop = io.EOF
		return rg, nil
	}, nil
}

func (c *Mysql) Insert(rb *row.RowsBuffer, columns []string) (affectedRows int64, err error) {
	if columns == nil || len(columns) == 0 {
		columns = make([]string, c.Metadata.GetColumnNumber())
		for i, column := range c.Metadata.Columns {
			columns[i] = column.ColumnName
		}
	}

	indexes := make([]int, len(columns))
	sqlCache := "INSERT INTO " + c.Config.Schema + "." + c.Config.Table + "( "
	for i, column := range columns {
		indexes[i], err = c.Metadata.GetIndexByName(column)
		if err != nil {
			return
		}
		sqlCache += "`" + column + "`,"
	}

	sqlCache = strings.TrimSuffix(sqlCache, ",") + ") VALUES "
	placeholder := strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",")
	dsn := c.getDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	defer db.Close()
	var rg *row.RowsGroup
	for {
		rg, err = rb.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		var vals []interface{}
		sqlStr := sqlCache
		for r := 0; r < rg.RowsNumber; r++ {
			for columnNum, index := range indexes {
				vals = append(vals, datatype.ToValue(rg.Vals[columnNum][r], c.Metadata.Columns[index].ColumnType))
			}

			sqlStr += "(" + placeholder + "),"
		}
		sqlStr = strings.TrimSuffix(sqlStr, ",")
		stmt, err := db.Prepare(sqlStr)
		if err != nil {
			return 0, err
		}

		result, err := stmt.Exec(vals...)
		if err != nil {
			return 0, err
		}
		insertedRows, err := result.RowsAffected()
		if err != nil {
			return affectedRows, err
		}

		affectedRows += insertedRows
	}

	return
}

func (c *Mysql) ShowSchemas(catalog string, _, _ *string) row.Reader {
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

func (c *Mysql) ShowTables(catalog, schema string, _, _ *string) row.Reader {
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

func (c *Mysql) ShowColumns(catalog, schema, table string) row.Reader {
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

func (c *Mysql) ShowPartitions(_, _, _ string) row.Reader {
	return func() (*row.Row, error) {
		return nil, io.EOF
	}
}

func (c *Mysql) getDSN() string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/", c.Config.User, c.Config.Password, c.Config.Host, c.Config.Port)
	return dsn
}
