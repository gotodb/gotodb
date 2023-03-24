package operator

import (
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
)

type QualifiedNameNode struct {
	Name string
}

func NewQualifiedNameNode(_ *config.Runtime, t parser.IQualifiedNameContext) *QualifiedNameNode {
	res := &QualifiedNameNode{}
	tt := t.(*parser.QualifiedNameContext)
	ids := tt.AllIdentifier()
	var names []string
	for i := 0; i < len(ids); i++ {
		id := ids[i].(*parser.IdentifierContext)
		names = append(names, id.GetText())
	}
	res.Name = strings.Join(names, ".")
	return res
}

func (n *QualifiedNameNode) Result() string {
	return n.Name
}

func (n *QualifiedNameNode) Init(md *metadata.Metadata) error {
	return nil
}
