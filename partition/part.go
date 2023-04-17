package partition

import (
	"bytes"

	"github.com/gotodb/gotodb/gtype"
)

type Part struct {
	Type   gtype.Type
	Vals   []interface{}
	Buffer []byte
}

func NewPart(t gtype.Type) *Part {
	return &Part{
		Type:   t,
		Vals:   []interface{}{},
		Buffer: []byte{},
	}
}

func (p *Part) Encode() {
	p.Buffer = gtype.EncodeValues(p.Vals, p.Type)
}

func (p *Part) Decode() (err error) {
	reader := bytes.NewReader(p.Buffer)
	p.Vals, err = gtype.DecodeValue(reader, p.Type)
	return err
}

func (p *Part) Append(val interface{}) {
	p.Vals = append(p.Vals, val)
}
