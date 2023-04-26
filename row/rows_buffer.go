package row

import (
	"bytes"
	"io"
	"sync"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/util"
)

const RowsBufferSize = 10000

type RowsBuffer struct {
	sync.Mutex
	MD         *metadata.Metadata
	BufferSize int
	RowsNumber int
	Index      int

	ValueBuffers  [][]interface{}
	ValueNilFlags [][]interface{} //bool

	KeyBuffers  [][]interface{}
	KeyNilFlags [][]interface{} //bool

	Reader io.Reader
	Writer io.Writer
}

func NewRowsBuffer(md *metadata.Metadata, reader io.Reader, writer io.Writer) *RowsBuffer {
	res := &RowsBuffer{
		MD:         md,
		BufferSize: RowsBufferSize,
		Reader:     reader,
		Writer:     writer,
	}
	res.ClearValues()
	return res
}

func (rb *RowsBuffer) ClearValues() {
	colNum := rb.MD.GetColumnNumber()
	rb.ValueBuffers = make([][]interface{}, colNum)
	rb.ValueNilFlags = make([][]interface{}, colNum)

	keyNum := rb.MD.GetKeyNumber()
	rb.KeyBuffers = make([][]interface{}, keyNum)
	rb.KeyNilFlags = make([][]interface{}, keyNum)
	rb.Index = 0
	rb.RowsNumber = 0

}

func (rb *RowsBuffer) Flush() error {
	rb.Lock()
	defer rb.Unlock()
	if err := rb.writeRows(); err != nil {
		return err
	}
	if err := util.WriteEOFMessage(rb.Writer); err != nil {
		return err
	}
	return nil
}

// Write rows
func (rb *RowsBuffer) writeRows() error {
	defer rb.ClearValues()
	ln := len(rb.ValueBuffers)

	//for 0 cols, just need send the number of rows
	if ln <= 0 {
		buf := datatype.EncodeValues([]interface{}{int64(rb.RowsNumber)}, datatype.INT64)
		return util.WriteMessage(rb.Writer, buf)
	}

	//for several cols
	for i := 0; i < ln; i++ {
		col := rb.ValueNilFlags[i]
		buf := datatype.EncodeBool(col)
		if err := util.WriteMessage(rb.Writer, buf); err != nil {
			return err
		}

		col = rb.ValueBuffers[i]
		t, err := rb.MD.GetTypeByIndex(i)
		if err != nil {
			return err
		}
		buf = datatype.EncodeValues(col, t)
		if err := util.WriteMessage(rb.Writer, buf); err != nil {
			return err
		}
	}

	ln = len(rb.KeyBuffers)
	for i := 0; i < ln; i++ {
		col := rb.KeyNilFlags[i]
		buf := datatype.EncodeBool(col)
		if err := util.WriteMessage(rb.Writer, buf); err != nil {
			return err
		}

		col = rb.KeyBuffers[i]
		t, err := rb.MD.GetKeyTypeByIndex(i)
		if err != nil {
			return err
		}
		buf = datatype.EncodeValues(col, t)
		if err := util.WriteMessage(rb.Writer, buf); err != nil {
			return err
		}
	}
	return nil
}

// read rows
func (rb *RowsBuffer) readRows() error {
	defer func() {
		rb.Index = 0
	}()

	colNum := rb.MD.GetColumnNumber()
	//for 0 cols
	if colNum <= 0 {
		buf, err := util.ReadMessage(rb.Reader)
		if err != nil {
			return err
		}
		vals, err := datatype.DecodeINT64(bytes.NewReader(buf))
		if err != nil || len(vals) <= 0 {
			return err
		}
		rb.RowsNumber = int(vals[0].(int64))
	}

	//for cols
	for i := 0; i < colNum; i++ {
		buf, err := util.ReadMessage(rb.Reader)
		if err != nil {
			return err
		}

		rb.ValueNilFlags[i], err = datatype.DecodeBOOL(bytes.NewReader(buf))
		if err != nil {
			return err
		}

		buf, err = util.ReadMessage(rb.Reader)
		if err != nil {
			return err
		}

		t, err := rb.MD.GetTypeByIndex(i)
		if err != nil {
			return err
		}
		values, err := datatype.DecodeValue(bytes.NewReader(buf), t)
		if err != nil {
			return err
		}

		rb.ValueBuffers[i] = make([]interface{}, len(rb.ValueNilFlags[i]))
		k := 0
		for j := 0; j < len(rb.ValueNilFlags[i]) && k < len(values); j++ {
			if rb.ValueNilFlags[i][j].(bool) {
				rb.ValueBuffers[i][j] = values[k]
				k++
			} else {
				rb.ValueBuffers[i][j] = nil
			}
		}

		rb.RowsNumber = len(rb.ValueNilFlags[i])

	}

	keyNum := rb.MD.GetKeyNumber()
	for i := 0; i < keyNum; i++ {
		buf, err := util.ReadMessage(rb.Reader)
		if err != nil {
			return err
		}
		rb.KeyNilFlags[i], err = datatype.DecodeBOOL(bytes.NewReader(buf))
		if err != nil {
			return err
		}

		buf, err = util.ReadMessage(rb.Reader)
		t, err := rb.MD.GetKeyTypeByIndex(i)
		if err != nil {
			return err
		}
		keys, err := datatype.DecodeValue(bytes.NewReader(buf), t)
		if err != nil {
			return err
		}

		rb.KeyBuffers[i] = make([]interface{}, len(rb.KeyNilFlags[i]))
		k := 0
		for j := 0; j < len(rb.KeyNilFlags[i]) && k < len(keys); j++ {
			if rb.KeyNilFlags[i][j].(bool) {
				rb.KeyBuffers[i][j] = keys[k]
				k++
			} else {
				rb.KeyBuffers[i][j] = nil
			}
		}
	}

	return nil

}

func (rb *RowsBuffer) WriteRow(rows ...*Row) error {
	rb.Lock()
	defer rb.Unlock()
	for _, row := range rows {
		for i, val := range row.Vals {
			if val != nil {
				rb.ValueBuffers[i] = append(rb.ValueBuffers[i], val)
				rb.ValueNilFlags[i] = append(rb.ValueNilFlags[i], true)
			} else {
				rb.ValueNilFlags[i] = append(rb.ValueNilFlags[i], false)
			}
		}

		for i, key := range row.Keys {
			if key != nil {
				rb.KeyBuffers[i] = append(rb.KeyBuffers[i], key)
				rb.KeyNilFlags[i] = append(rb.KeyNilFlags[i], true)
			} else {
				rb.KeyNilFlags[i] = append(rb.KeyNilFlags[i], false)
			}
		}
		rb.RowsNumber++

		if rb.RowsNumber >= rb.BufferSize {
			if err := rb.writeRows(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rb *RowsBuffer) ReadRow() (*Row, error) {
	rb.Lock()
	defer rb.Unlock()

	for rb.Index >= rb.RowsNumber {
		rb.ClearValues()
		if err := rb.readRows(); err != nil {
			return nil, err
		}
	}

	row := Pool.Get().(*Row)
	row.Clear()
	row.Vals = make([]interface{}, len(rb.ValueBuffers))
	for i, col := range rb.ValueBuffers {
		row.Vals[i] = col[rb.Index]
	}

	row.Keys = make([]interface{}, len(rb.KeyBuffers))
	for i, col := range rb.KeyBuffers {
		row.Keys[i] = col[rb.Index]
	}
	rb.Index++
	return row, nil
}

func (rb *RowsBuffer) Write(rg *RowsGroup) error {
	rb.Lock()
	defer rb.Unlock()
	for i, vs := range rg.Vals {
		for _, v := range vs {
			if v != nil {
				rb.ValueBuffers[i] = append(rb.ValueBuffers[i], v)
				rb.ValueNilFlags[i] = append(rb.ValueNilFlags[i], true)
			} else {
				rb.ValueNilFlags[i] = append(rb.ValueNilFlags[i], false)
			}
		}
	}

	for i, ks := range rg.Keys {
		for _, k := range ks {
			if k != nil {
				rb.KeyBuffers[i] = append(rb.KeyBuffers[i], k)
				rb.KeyNilFlags[i] = append(rb.KeyNilFlags[i], true)
			} else {
				rb.KeyNilFlags[i] = append(rb.KeyNilFlags[i], false)
			}
		}
	}
	rb.RowsNumber += rg.RowsNumber

	if rb.RowsNumber >= rb.BufferSize {
		if err := rb.writeRows(); err != nil {
			return err
		}
	}
	return nil
}

func (rb *RowsBuffer) Read() (*RowsGroup, error) {
	rb.Lock()
	defer rb.Unlock()

	for rb.Index >= rb.RowsNumber {
		rb.ClearValues()
		if err := rb.readRows(); err != nil {
			return nil, err
		}
	}

	rg := NewRowsGroup(rb.MD)
	readSize := rb.BufferSize
	if readSize > rb.RowsNumber-rb.Index {
		readSize = rb.RowsNumber - rb.Index
	}

	for i := 0; i < len(rg.Vals); i++ {
		rg.Vals[i] = append(rg.Vals[i], rb.ValueBuffers[i][rb.Index:rb.Index+readSize]...)
	}

	for i := 0; i < len(rg.Keys); i++ {
		rg.Keys[i] = append(rg.Keys[i], rb.KeyBuffers[i][rb.Index:rb.Index+readSize]...)
	}

	rb.Index += readSize
	rg.RowsNumber = readSize

	return rg, nil
}
