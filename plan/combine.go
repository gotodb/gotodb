package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type CombineNode struct {
	Inputs   []Node
	Output   Node
	Metadata *metadata.Metadata
}

func NewCombineNode(_ *config.Runtime, inputs []Node) *CombineNode {
	return &CombineNode{
		Inputs:   inputs,
		Metadata: metadata.NewMetadata(),
	}
}

func (n *CombineNode) GetInputs() []Node {
	return n.Inputs
}

func (n *CombineNode) SetInputs(inputs []Node) {
	n.Inputs = inputs
}

func (n *CombineNode) GetOutput() Node {
	return n.Output
}

func (n *CombineNode) SetOutput(output Node) {
	n.Output = output
}

func (n *CombineNode) GetType() NodeType {
	return NodeTypeCombine
}

func (n *CombineNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *CombineNode) SetMetadata() (err error) {
	n.Metadata = metadata.NewMetadata()
	for _, input := range n.Inputs {
		if err = input.SetMetadata(); err != nil {
			return err
		}
		n.Metadata = metadata.JoinMetadata(n.Metadata, input.GetMetadata())
	}
	return nil
}

func (n *CombineNode) String() string {
	res := "CombineNode {\n"
	for _, n := range n.Inputs {
		res += n.String()
	}
	res += "}\n"
	return res
}
