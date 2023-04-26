package operator

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/row"
)

type StringValueNode struct {
	Name string
	Str  string
}

func NewStringValueNode(_ *config.Runtime, t parser.IStringValueContext) *StringValueNode {
	s := t.GetText()
	ls := len(s)
	return &StringValueNode{
		Str:  s[1 : ls-1],
		Name: s[1 : ls-1],
	}
}

func (n *StringValueNode) Init(_ *metadata.Metadata) error {
	return nil
}

func (n *StringValueNode) Result(input *row.RowsGroup) (interface{}, error) {
	rn := input.GetRowsNumber()
	res := make([]interface{}, rn)
	for i := 0; i < rn; i++ {
		res[i] = n.Str
	}
	return res, nil
}

func (n *StringValueNode) GetType(_ *metadata.Metadata) (datatype.Type, error) {
	return datatype.STRING, nil
}

func (n *StringValueNode) IsAggregate() bool {
	return false
}

func (n *StringValueNode) GetText() string {
	return n.Str
}
