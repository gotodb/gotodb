package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/planner/operator"
)

type FilterPlan struct {
	Input              Plan
	Output             Plan
	Metadata           *metadata.Metadata
	BooleanExpressions []*operator.BooleanExpressionNode
}

func NewFilterPlan(runtime *config.Runtime, input Plan, t parser.IBooleanExpressionContext) *FilterPlan {
	res := &FilterPlan{
		Input:              input,
		Metadata:           metadata.NewMetadata(),
		BooleanExpressions: []*operator.BooleanExpressionNode{operator.NewBooleanExpressionNode(runtime, t)},
	}
	return res
}
func (n *FilterPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *FilterPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *FilterPlan) GetOutput() Plan {
	return n.Output
}

func (n *FilterPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *FilterPlan) GetType() NodeType {
	return NodeTypeFilter
}

func (n *FilterPlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *FilterPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *FilterPlan) String() string {
	res := "FilterPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "BooleanExpressions: " + fmt.Sprint(n.BooleanExpressions) + "\n"
	res += "}\n"
	return res
}

func (n *FilterPlan) AddBooleanExpressions(nodes ...*operator.BooleanExpressionNode) {
	n.BooleanExpressions = append(n.BooleanExpressions, nodes...)
}
