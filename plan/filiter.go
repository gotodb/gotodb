package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/plan/operator"
)

type FilterNode struct {
	Input              Node
	Output             Node
	Metadata           *metadata.Metadata
	BooleanExpressions []*operator.BooleanExpressionNode
}

func NewFilterNode(runtime *config.Runtime, input Node, t parser.IBooleanExpressionContext) *FilterNode {
	res := &FilterNode{
		Input:              input,
		Metadata:           metadata.NewMetadata(),
		BooleanExpressions: []*operator.BooleanExpressionNode{operator.NewBooleanExpressionNode(runtime, t)},
	}
	return res
}
func (n *FilterNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *FilterNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *FilterNode) GetOutput() Node {
	return n.Output
}

func (n *FilterNode) SetOutput(output Node) {
	n.Output = output
}

func (n *FilterNode) GetType() NodeType {
	return NodeTypeFilter
}

func (n *FilterNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *FilterNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *FilterNode) String() string {
	res := "FilterNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "BooleanExpressions: " + fmt.Sprint(n.BooleanExpressions) + "\n"
	res += "}\n"
	return res
}

func (n *FilterNode) AddBooleanExpressions(nodes ...*operator.BooleanExpressionNode) {
	n.BooleanExpressions = append(n.BooleanExpressions, nodes...)
}
