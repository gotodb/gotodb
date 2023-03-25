package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type UseNode struct {
	Catalog, Schema string
}

func NewUseNode(_ *config.Runtime, ct, sh string) *UseNode {
	return &UseNode{
		Catalog: ct,
		Schema:  sh,
	}
}

func (n *UseNode) GetType() NodeType {
	return NodeTypeUse
}

func (n *UseNode) SetMetadata() error {
	return nil
}

func (n *UseNode) GetMetadata() *metadata.Metadata {
	return nil
}

func (n *UseNode) GetOutput() Node {
	return nil
}

func (n *UseNode) SetOutput(_ Node) {
	return
}

func (n *UseNode) GetInputs() []Node {
	return nil
}

func (n *UseNode) SetInputs(_ []Node) {
	return
}

func (n *UseNode) String() string {
	res := "UseNode  {\n"
	res += n.Catalog + "." + n.Schema
	res += "}\n"
	return res
}
