package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type AggregatePlan struct {
	Input    Plan
	Output   Plan
	Metadata *metadata.Metadata
}

func NewAggregatePlan(_ *config.Runtime, input Plan) *AggregatePlan {
	return &AggregatePlan{
		Input:    input,
		Metadata: metadata.NewMetadata(),
	}
}

func (n *AggregatePlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *AggregatePlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *AggregatePlan) GetOutput() Plan {
	return n.Output
}

func (n *AggregatePlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *AggregatePlan) GetType() NodeType {
	return NodeTypeAggregate
}

func (n *AggregatePlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *AggregatePlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()

	return nil
}

func (n *AggregatePlan) String() string {
	res := "AggregatePlan {\n"
	res += n.Input.String()
	res += "}\n"
	return res
}
