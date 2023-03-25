package plan

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type UnionType int32

const (
	_ UnionType = iota
	INTERSECT
	UNION
	EXCEPT
)

type UnionNode struct {
	LeftInput  Node
	RightInput Node
	Output     Node
	Operator   UnionType
	Metadata   *metadata.Metadata
}

func NewUnionNode(_ *config.Runtime, left, right Node, op antlr.Token) *UnionNode {
	var operator UnionType
	switch op.GetText() {
	case "INTERSECT":
		operator = INTERSECT
	case "UNION":
		operator = UNION
	case "EXCEPT":
		operator = EXCEPT
	}

	res := &UnionNode{
		LeftInput:  left,
		RightInput: right,
		Operator:   operator,
		Metadata:   metadata.NewMetadata(),
	}
	return res
}

func (n *UnionNode) GetInputs() []Node {
	return []Node{n.LeftInput, n.RightInput}
}

func (n *UnionNode) SetInputs(inputs []Node) {
	n.LeftInput, n.RightInput = inputs[0], inputs[1]
}

func (n *UnionNode) GetOutput() Node {
	return n.Output
}

func (n *UnionNode) SetOutput(output Node) {
	n.Output = output
}

func (n *UnionNode) GetType() NodeType {
	return NodeTypeUnion
}

func (n *UnionNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *UnionNode) SetMetadata() (err error) {
	if err = n.LeftInput.SetMetadata(); err != nil {
		return err
	}
	if err = n.RightInput.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.LeftInput.GetMetadata().Copy()
	return nil
}

func (n *UnionNode) String() string {
	res := "UnionNode {\n"
	res += "LeftInput: " + n.LeftInput.String() + "\n"
	res += "RightInput: " + n.RightInput.String() + "\n"
	res += "Operator: " + fmt.Sprint(n.Operator) + "\n"
	res += "}\n"
	return res
}
