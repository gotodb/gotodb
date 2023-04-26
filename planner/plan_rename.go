package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type RenamePlan struct {
	Rename   string
	Metadata *metadata.Metadata
	Input    Plan
	Output   Plan
}

func NewRenamePlan(_ *config.Runtime, input Plan, rename string) *RenamePlan {
	return &RenamePlan{
		Rename:   rename,
		Metadata: metadata.NewMetadata(),
		Input:    input,
	}
}

func (n *RenamePlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *RenamePlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *RenamePlan) GetOutput() Plan {
	return n.Output
}

func (n *RenamePlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *RenamePlan) GetType() NodeType {
	return NodeTypeRename
}

func (n *RenamePlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *RenamePlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	n.Metadata.Rename(n.Rename)
	return nil
}

func (n *RenamePlan) String() string {
	res := "RenamePlan {\n"
	res += "Rename: " + n.Rename + "\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "}\n"
	return res
}
