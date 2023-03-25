package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type RenameNode struct {
	Rename   string
	Metadata *metadata.Metadata
	Input    Node
	Output   Node
}

func NewRenameNode(_ *config.Runtime, input Node, tname string) *RenameNode {
	return &RenameNode{
		Rename:   tname,
		Metadata: metadata.NewMetadata(),
		Input:    input,
	}
}

func (n *RenameNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *RenameNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *RenameNode) GetOutput() Node {
	return n.Output
}

func (n *RenameNode) SetOutput(output Node) {
	n.Output = output
}

func (n *RenameNode) GetType() NodeType {
	return NodeTypeRename
}

func (n *RenameNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *RenameNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	n.Metadata.Rename(n.Rename)
	return nil
}

func (n *RenameNode) String() string {
	res := "RenameNode {\n"
	res += "Rename: " + n.Rename + "\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "}\n"
	return res
}
