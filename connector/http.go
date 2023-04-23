package connector

import (
	"encoding/json"
	"fmt"
	"github.com/gotodb/gotodb/partition"
	"github.com/gotodb/gotodb/plan/operator"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Http struct {
	Config    *config.HttpConnector
	Metadata  *metadata.Metadata
	Partition *partition.Partition
}

func NewHttpConnectorEmpty() *Http {
	return &Http{}
}

func NewHttpConnector(catalog, schema, table string) (*Http, error) {
	var err error
	res := &Http{}
	key := strings.Join([]string{catalog, schema, table}, ".")
	conf := config.Conf.HttpConnectors.GetConfig(key)
	if conf == nil {
		return nil, fmt.Errorf("http connector: table not found")
	}
	res.Config = conf
	res.Metadata, err = NewHttpMetadata(conf)

	return res, err
}

func NewHttpMetadata(conf *config.HttpConnector) (*metadata.Metadata, error) {
	res := metadata.NewMetadata()
	conf.ColumnNames = append(conf.ColumnNames, conf.FilterColumn, conf.ResultColumn)
	conf.ColumnTypes = append(conf.ColumnTypes, "STRING", "STRING")
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

func (c *Http) GetMetadata() (*metadata.Metadata, error) {
	return c.Metadata, nil
}

func (c *Http) GetPartition(partitionNumber int) (*partition.Partition, error) {
	if c.Partition == nil {
		c.Partition = partition.New(metadata.NewMetadata())
		for i := 0; i < partitionNumber; i++ {
			c.Partition.Locations = append(c.Partition.Locations, fmt.Sprintf("%d/%d", i, partitionNumber))
		}
	}
	return c.Partition, nil
}

func (c *Http) GetReader(file *partition.FileLocation, md *metadata.Metadata, filters []*operator.BooleanExpressionNode) (row.GroupReader, error) {
	var _http string
	for _, filter := range filters {
		if filter.Name == c.Config.FilterColumn {
			_http = filter.Predicated.Predicate.FirstValueExpression.PrimaryExpression.StringValue.Str
			break
		}
	}
	var part, partitionNumber int
	_, _ = fmt.Sscanf(file.Location, "%d/%d", &part, &partitionNumber)

	type Options struct {
		URL         string        `json:"url"`
		URI         string        `json:"uri"`
		DataPath    string        `json:"dataPath"`
		Timeout     time.Duration `json:"timeout"`
		Method      string        `json:"method"`
		PartitionBy string        `json:"partitionBy"`
		ContentType string        `json:"contentType"`
		Body        string        `json:"body"`
	}

	var options Options
	if err := json.Unmarshal([]byte(_http), &options); err != nil {
		return nil, err
	}

	body := ""
	if options.PartitionBy != "" {
		if options.Body != "" {
			switch options.ContentType {
			case "application/x-www-form-urlencoded":
				uri, err := url.ParseQuery(options.Body)
				if err != nil {
					return nil, err
				}
				p := uri.Get(options.PartitionBy)
				if p == "" {
					return nil, fmt.Errorf("no partition key in body")
				}
				var iValues []interface{}
				if err := json.Unmarshal([]byte(p), &iValues); err != nil {
					return nil, err
				}

				if len(iValues) < partitionNumber {
					return nil, fmt.Errorf("insufficient partition value length, %s < partition number(%d)", options.PartitionBy, partitionNumber)
				}

				pIndex := len(iValues) / partitionNumber
				if partitionNumber == part {
					// last part
					iValues = iValues[pIndex*part:]
				} else {
					iValues = iValues[pIndex*part : pIndex*(part+1)]
				}
				bytes, _ := json.Marshal(&iValues)
				uri.Set(options.PartitionBy, string(bytes))
				body = uri.Encode()
			default:
				var iBody map[string]interface{}
				if err := json.Unmarshal([]byte(options.Body), &iBody); err != nil {
					return nil, err
				}
				p, ok := iBody[options.PartitionBy]
				if !ok {
					return nil, fmt.Errorf("no partition key in body")
				}
				iValues := p.([]interface{})
				if len(iValues) < partitionNumber {
					return nil, fmt.Errorf("insufficient partition value length, %s < partition number(%d)", options.PartitionBy, partitionNumber)
				}

				index := len(iValues) / partitionNumber
				if partitionNumber == part {
					// last part
					iValues = iValues[index*part:]
				} else {
					iValues = iValues[index*part : index*(part+1)]
				}

				iBody[options.PartitionBy] = iValues
				bytes, _ := json.Marshal(&iBody)
				body = string(bytes)
			}
		} else if options.URI != "" {
			uri, err := url.ParseQuery(options.URI)
			if err != nil {
				return nil, err
			}
			p := uri.Get(options.PartitionBy)
			if p == "" {
				return nil, fmt.Errorf("no partition key in uri")
			}
			var iValues []interface{}
			if err := json.Unmarshal([]byte(p), &iValues); err != nil {
				return nil, err
			}

			if len(iValues) < partitionNumber {
				return nil, fmt.Errorf("insufficient partition value length, %s < partition number(%d)", options.PartitionBy, partitionNumber)
			}

			pIndex := len(iValues) / partitionNumber
			if partitionNumber == part {
				// last part
				iValues = iValues[pIndex*part:]
			} else {
				iValues = iValues[pIndex*part : pIndex*(part+1)]
			}
			bytes, _ := json.Marshal(&iValues)
			uri.Set(options.PartitionBy, string(bytes))
			options.URI = uri.Encode()
		} else {
			return nil, fmt.Errorf("no partition key in body or uri")
		}
	}

	var stop error
	return func(indexes []int) (*row.RowsGroup, error) {
		if stop != nil {
			return nil, stop
		}

		client := &http.Client{}
		if options.Timeout > 0 {
			client.Timeout = options.Timeout * time.Millisecond
		}

		separator := "?"
		if strings.HasSuffix(options.URL, "?") {
			separator = "&"
		} else if strings.HasSuffix(options.URL, "&") || options.URI == "" {
			separator = ""
		}

		u := options.URL + separator + options.URI
		req, err := http.NewRequest(strings.ToUpper(options.Method), u, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()
		var iResp interface{}
		switch resp.Header.Get("Content-Type") {
		case "application/json":
			if err := json.Unmarshal(respBody, &iResp); err != nil {
				return nil, err
			}
		}

		rg := row.NewRowsGroup(md.SelectColumnsByIndexes(indexes))
		var iData = iResp
		if options.DataPath != "" {
			dataPaths := strings.Split(options.DataPath, ".")
			for _, path := range dataPaths {
				if data, ok := iData.(map[string]interface{}); !ok {
					return rg, fmt.Errorf("response data type error, not json object %v", iResp)
				} else if iData, ok = data[path]; !ok {
					return rg, fmt.Errorf("response data path error, %v not exsits data path %s", iResp, path)
				}
			}

		}

	typeAssert:
		switch iData.(type) {
		case map[string]interface{}:
			iData = []interface{}{iData}
			goto typeAssert
		case []interface{}:
			for _, item := range iData.([]interface{}) {
				var data map[string]interface{}
				var ok bool
				if data, ok = item.(map[string]interface{}); !ok {
					return rg, fmt.Errorf("response data type error, can not assert %v", item)
				}
				for _, index := range indexes {
					col := rg.Metadata.Columns[index]
					if col.ColumnName == c.Config.FilterColumn {
						rg.Vals[index] = append(rg.Vals[index], _http)
					} else if col.ColumnName == c.Config.ResultColumn {
						rg.Vals[index] = append(rg.Vals[index], string(respBody))
					} else if value, ok := data[col.ColumnName]; ok {
						rg.Vals[index] = append(rg.Vals[index], gtype.ToType(value, md.Columns[index].ColumnType))
					} else {
						rg.Vals[index] = append(rg.Vals[index], nil)
					}
				}
				rg.RowsNumber++
			}
		}

		stop = io.EOF
		return rg, nil
	}, nil
}

func (c *Http) Insert(rb *row.RowsBuffer, Columns []string) (affectedRows int64, err error) {
	for {
		rg, err := rb.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		affectedRows += int64(rg.RowsNumber)
	}

	return
}

func (c *Http) ShowSchemas(catalog string, _, _ *string) row.Reader {
	var err error
	var rs []*row.Row
	for key := range config.Conf.HttpConnectors {
		ns := strings.Split(key, ".")
		c, s, _ := ns[0], ns[1], ns[2]
		if c == catalog {
			r := row.NewRow()
			r.AppendVals(s)
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

func (c *Http) ShowTables(catalog, schema string, _, _ *string) row.Reader {
	var err error
	var rs []*row.Row
	for key := range config.Conf.HttpConnectors {
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

func (c *Http) ShowColumns(catalog, schema, table string) row.Reader {
	var err error
	var rs []*row.Row
	for key, conf := range config.Conf.HttpConnectors {
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

func (c *Http) ShowPartitions(_, _, _ string) row.Reader {
	return func() (*row.Row, error) {
		return nil, io.EOF
	}
}
