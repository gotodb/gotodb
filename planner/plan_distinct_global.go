package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type DistinctGlobalPlan struct {
	Input       Plan
	Output      Plan
	Metadata    *metadata.Metadata
	Expressions []*ExpressionNode
}

func NewDistinctGlobalPlan(_ *config.Runtime, eps []*ExpressionNode, input Plan) *DistinctGlobalPlan {
	res := &DistinctGlobalPlan{
		Input:       input,
		Metadata:    metadata.NewMetadata(),
		Expressions: eps,
	}
	return res
}
func (n *DistinctGlobalPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *DistinctGlobalPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *DistinctGlobalPlan) GetOutput() Plan {
	return n.Output
}

func (n *DistinctGlobalPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *DistinctGlobalPlan) GetType() NodeType {
	return NodeTypeDistinctGlobal
}

func (n *DistinctGlobalPlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *DistinctGlobalPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *DistinctGlobalPlan) String() string {
	res := "DistinctGlobalPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Expressions: " + fmt.Sprint(n.Expressions) + "\n"
	res += "}\n"
	return res
}

func (n *DistinctGlobalPlan) AddExpressions(nodes ...*ExpressionNode) {
	n.Expressions = append(n.Expressions, nodes...)
}
