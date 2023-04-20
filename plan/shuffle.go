package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/plan/operator"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type ShuffleNode struct {
	Input              Node
	Output             Node
	Metadata           *metadata.Metadata
	BooleanExpressions []*operator.BooleanExpressionNode
}

func NewShuffleNode(runtime *config.Runtime, input Node, t parser.IBooleanExpressionContext) *ShuffleNode {
	res := &ShuffleNode{
		Input:              input,
		Metadata:           metadata.NewMetadata(),
		BooleanExpressions: []*operator.BooleanExpressionNode{operator.NewBooleanExpressionNode(runtime, t)},
	}
	return res
}
func (n *ShuffleNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *ShuffleNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *ShuffleNode) GetOutput() Node {
	return n.Output
}

func (n *ShuffleNode) SetOutput(output Node) {
	n.Output = output
}

func (n *ShuffleNode) GetType() NodeType {
	return NodeTypeShuffle
}

func (n *ShuffleNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *ShuffleNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *ShuffleNode) String() string {
	res := "ShuffleNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "BooleanExpressions: " + fmt.Sprint(n.BooleanExpressions) + "\n"
	res += "}\n"
	return res
}

func (n *ShuffleNode) AddBooleanExpressions(nodes ...*operator.BooleanExpressionNode) {
	n.BooleanExpressions = append(n.BooleanExpressions, nodes...)
}
