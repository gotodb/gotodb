package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type DistinctGlobalNode struct {
	Input       Node
	Output      Node
	Metadata    *metadata.Metadata
	Expressions []*operator.ExpressionNode
}

func NewDistinctGlobalNode(runtime *config.Runtime, eps []*operator.ExpressionNode, input Node) *DistinctGlobalNode {
	res := &DistinctGlobalNode{
		Input:       input,
		Metadata:    metadata.NewMetadata(),
		Expressions: eps,
	}
	return res
}
func (n *DistinctGlobalNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *DistinctGlobalNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *DistinctGlobalNode) GetOutput() Node {
	return n.Output
}

func (n *DistinctGlobalNode) SetOutput(output Node) {
	n.Output = output
}

func (n *DistinctGlobalNode) GetType() NodeType {
	return NodeTypeDistinctGlobal
}

func (n *DistinctGlobalNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *DistinctGlobalNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *DistinctGlobalNode) String() string {
	res := "DistinctGlobalNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Expressions: " + fmt.Sprint(n.Expressions) + "\n"
	res += "}\n"
	return res
}

func (n *DistinctGlobalNode) AddExpressions(nodes ...*operator.ExpressionNode) {
	n.Expressions = append(n.Expressions, nodes...)
}
