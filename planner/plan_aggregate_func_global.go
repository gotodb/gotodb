package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/planner/operator"
)

type AggregateFuncGlobalPlan struct {
	Input     Plan
	Output    Plan
	FuncNodes []*operator.FuncCallNode
	Metadata  *metadata.Metadata
}

func NewAggregateFuncGlobalPlan(_ *config.Runtime, funcs []*operator.FuncCallNode, input Plan) *AggregateFuncGlobalPlan {
	return &AggregateFuncGlobalPlan{
		Input:     input,
		FuncNodes: funcs,
		Metadata:  metadata.NewMetadata(),
	}
}

func (n *AggregateFuncGlobalPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *AggregateFuncGlobalPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *AggregateFuncGlobalPlan) GetOutput() Plan {
	return n.Output
}

func (n *AggregateFuncGlobalPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *AggregateFuncGlobalPlan) GetType() NodeType {
	return NodeTypeAggregateFuncGlobal
}

func (n *AggregateFuncGlobalPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *AggregateFuncGlobalPlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *AggregateFuncGlobalPlan) String() string {
	res := "AggregateFuncGlobalPlan {\n"
	res += n.Input.String()
	res += "}\n"
	return res
}
