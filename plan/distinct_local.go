package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type DistinctLocalNode struct {
	Input       Node
	Output      Node
	Metadata    *metadata.Metadata
	Expressions []*operator.ExpressionNode
}

func NewDistinctLocalNode(_ *config.Runtime, eps []*operator.ExpressionNode, input Node) *DistinctLocalNode {
	res := &DistinctLocalNode{
		Input:       input,
		Metadata:    metadata.NewMetadata(),
		Expressions: eps,
	}
	return res
}
func (n *DistinctLocalNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *DistinctLocalNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *DistinctLocalNode) GetOutput() Node {
	return n.Output
}

func (n *DistinctLocalNode) SetOutput(output Node) {
	n.Output = output
}

func (n *DistinctLocalNode) GetType() NodeType {
	return DISTINCTLOCALNODE
}

func (n *DistinctLocalNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	for _, e := range n.Expressions {
		t, err := e.GetType(n.Input.GetMetadata())
		if err != nil {
			return err
		}
		col := metadata.NewColumnMetadata(t, e.Name)
		n.Metadata.AppendColumn(col)
	}

	return nil
}

func (n *DistinctLocalNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *DistinctLocalNode) String() string {
	res := "DistinctLocalNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Expressions: " + fmt.Sprint(n.Expressions) + "\n"
	res += "}\n"
	return res
}

func (n *DistinctLocalNode) AddExpressions(nodes ...*operator.ExpressionNode) {
	n.Expressions = append(n.Expressions, nodes...)
}
