package operator

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type BooleanValueNode struct {
	Name string
	Bool bool
}

func NewBooleanValueNode(runtime *config.Runtime, t parser.IBooleanValueContext) *BooleanValueNode {
	s := t.GetText()
	b := true
	if s != "TRUE" {
		b = false
	}
	return &BooleanValueNode{
		Bool: b,
		Name: s,
	}
}

func (n *BooleanValueNode) Init(md *metadata.Metadata) error {
	return nil
}

func (n *BooleanValueNode) Result(input *row.RowsGroup) (interface{}, error) {
	rn := input.GetRowsNumber()
	res := make([]interface{}, rn)
	for i := 0; i < rn; i++ {
		res[i] = n.Bool
	}
	return res, nil
}

func (n *BooleanValueNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	return gtype.BOOL, nil
}

func (n *BooleanValueNode) IsAggregate() bool {
	return false
}

func (n *BooleanValueNode) GetText() string {
	return n.Name
}
