package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type CombinePlan struct {
	Inputs   []Plan
	Output   Plan
	Metadata *metadata.Metadata
}

func NewCombinePlan(_ *config.Runtime, inputs []Plan) *CombinePlan {
	return &CombinePlan{
		Inputs:   inputs,
		Metadata: metadata.NewMetadata(),
	}
}

func (n *CombinePlan) GetInputs() []Plan {
	return n.Inputs
}

func (n *CombinePlan) SetInputs(inputs []Plan) {
	n.Inputs = inputs
}

func (n *CombinePlan) GetOutput() Plan {
	return n.Output
}

func (n *CombinePlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *CombinePlan) GetType() NodeType {
	return NodeTypeCombine
}

func (n *CombinePlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *CombinePlan) SetMetadata() (err error) {
	n.Metadata = metadata.NewMetadata()
	for _, input := range n.Inputs {
		if err = input.SetMetadata(); err != nil {
			return err
		}
		n.Metadata = metadata.JoinMetadata(n.Metadata, input.GetMetadata())
	}
	return nil
}

func (n *CombinePlan) String() string {
	res := "CombinePlan {\n"
	for _, n := range n.Inputs {
		res += n.String()
	}
	res += "}\n"
	return res
}
