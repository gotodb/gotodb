package plan

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type LimitNode struct {
	Input       Node
	Output      Node
	Metadata    *metadata.Metadata
	LimitNumber *int64
}

func NewLimitNode(_ *config.Runtime, input Node, t antlr.TerminalNode) *LimitNode {
	res := &LimitNode{
		Input:    input,
		Metadata: metadata.NewMetadata(),
	}
	if ns := t.GetText(); ns != "ALL" {
		var num int64
		fmt.Sscanf(ns, "%d", &num)
		res.LimitNumber = &num
	}
	return res
}

func (n *LimitNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *LimitNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *LimitNode) GetOutput() Node {
	return n.Output
}

func (n *LimitNode) SetOutput(output Node) {
	n.Output = output
}

func (n *LimitNode) GetType() NodeType {
	return NodeTypeLimit
}

func (n *LimitNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *LimitNode) SetMetadata() error {
	if err := n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *LimitNode) String() string {
	res := "LimitNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "LimitNubmer: " + fmt.Sprint(*n.LimitNumber) + "\n"
	res += "}\n"
	return res
}
