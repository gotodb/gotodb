package partition

import (
	"bytes"

	"github.com/gotodb/gotodb/datatype"
)

type Part struct {
	Type   datatype.Type
	Vals   []interface{}
	Buffer []byte
}

func NewPart(t datatype.Type) *Part {
	return &Part{
		Type:   t,
		Vals:   []interface{}{},
		Buffer: []byte{},
	}
}

func (p *Part) Encode() {
	p.Buffer = datatype.EncodeValues(p.Vals, p.Type)
}

func (p *Part) Decode() (err error) {
	reader := bytes.NewReader(p.Buffer)
	p.Vals, err = datatype.DecodeValue(reader, p.Type)
	return err
}

func (p *Part) Append(val interface{}) {
	p.Vals = append(p.Vals, val)
}
