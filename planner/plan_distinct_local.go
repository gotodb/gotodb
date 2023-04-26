package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/planner/operator"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type DistinctLocalPlan struct {
	Input       Plan
	Output      Plan
	Metadata    *metadata.Metadata
	Expressions []*operator.ExpressionNode
}

func NewDistinctLocalPlan(_ *config.Runtime, eps []*operator.ExpressionNode, input Plan) *DistinctLocalPlan {
	res := &DistinctLocalPlan{
		Input:       input,
		Metadata:    metadata.NewMetadata(),
		Expressions: eps,
	}
	return res
}
func (n *DistinctLocalPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *DistinctLocalPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *DistinctLocalPlan) GetOutput() Plan {
	return n.Output
}

func (n *DistinctLocalPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *DistinctLocalPlan) GetType() NodeType {
	return NodeTypeDistinctLocal
}

func (n *DistinctLocalPlan) SetMetadata() (err error) {
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

func (n *DistinctLocalPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *DistinctLocalPlan) String() string {
	res := "DistinctLocalPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Expressions: " + fmt.Sprint(n.Expressions) + "\n"
	res += "}\n"
	return res
}

func (n *DistinctLocalPlan) AddExpressions(nodes ...*operator.ExpressionNode) {
	n.Expressions = append(n.Expressions, nodes...)
}
