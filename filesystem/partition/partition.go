package partition

import (
	"bytes"

	"github.com/gotodb/gotodb/gtype"
)

type Partition struct {
	Type   gtype.Type
	Vals   []interface{}
	Buffer []byte
}

func NewPartition(t gtype.Type) *Partition {
	return &Partition{
		Type:   t,
		Vals:   []interface{}{},
		Buffer: []byte{},
	}
}

func (p *Partition) Encode() {
	p.Buffer = gtype.EncodeValues(p.Vals, p.Type)
}

func (p *Partition) Decode() (err error) {
	reader := bytes.NewReader(p.Buffer)
	p.Vals, err = gtype.DecodeValue(reader, p.Type)
	return err
}

func (p *Partition) Append(val interface{}) {
	p.Vals = append(p.Vals, val)
}
