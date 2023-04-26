package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/planner/operator"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type ShufflePlan struct {
	Input              Plan
	Output             Plan
	Metadata           *metadata.Metadata
	BooleanExpressions []*operator.BooleanExpressionNode
}

func NewShufflePlan(runtime *config.Runtime, input Plan, t parser.IBooleanExpressionContext) *ShufflePlan {
	res := &ShufflePlan{
		Input:              input,
		Metadata:           metadata.NewMetadata(),
		BooleanExpressions: []*operator.BooleanExpressionNode{operator.NewBooleanExpressionNode(runtime, t)},
	}
	return res
}
func (n *ShufflePlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *ShufflePlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *ShufflePlan) GetOutput() Plan {
	return n.Output
}

func (n *ShufflePlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *ShufflePlan) GetType() NodeType {
	return NodeTypeShuffle
}

func (n *ShufflePlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *ShufflePlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *ShufflePlan) String() string {
	res := "ShufflePlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "BooleanExpressions: " + fmt.Sprint(n.BooleanExpressions) + "\n"
	res += "}\n"
	return res
}

func (n *ShufflePlan) AddBooleanExpressions(nodes ...*operator.BooleanExpressionNode) {
	n.BooleanExpressions = append(n.BooleanExpressions, nodes...)
}
