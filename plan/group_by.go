package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/plan/operator"
)

type GroupByNode struct {
	Input    Node
	Output   Node
	Metadata *metadata.Metadata
	GroupBy  *operator.GroupByNode
}

func NewGroupByNode(runtime *config.Runtime, input Node, groupBy parser.IGroupByContext) *GroupByNode {
	return &GroupByNode{
		Input:    input,
		Metadata: metadata.NewMetadata(),
		GroupBy:  operator.NewGroupByNode(runtime, groupBy),
	}
}

func (n *GroupByNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *GroupByNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *GroupByNode) GetOutput() Node {
	return n.Output
}

func (n *GroupByNode) SetOutput(output Node) {
	n.Output = output
}

func (n *GroupByNode) GetType() NodeType {
	return NodeTypeGroupBy
}

func (n *GroupByNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	n.Metadata.AppendKeyByType(gtype.STRING)
	return nil
}

func (n *GroupByNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *GroupByNode) String() string {
	res := "GroupByNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "GroupBy: " + fmt.Sprint(n.GroupBy) + "\n"
	res += "}/n"
	return res
}
