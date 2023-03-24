package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
)

type UnionJob struct {
	Location              pb.Location
	LeftInput, RightInput pb.Location
	Output                pb.Location
	Operator              plan.UnionType
	Metadata              *metadata.Metadata
}

func (n *UnionJob) GetType() JobType {
	return JobTypeUnion
}

func (n *UnionJob) GetInputs() []pb.Location {
	return []pb.Location{n.LeftInput, n.RightInput}
}

func (n *UnionJob) GetOutputs() []pb.Location {
	return []pb.Location{n.Output}
}

func (n *UnionJob) GetLocation() pb.Location {
	return n.Location
}

func NewUnionJob(node *plan.UnionNode, leftInput, rightInput pb.Location, output pb.Location) *UnionJob {
	return &UnionJob{
		Location:   output,
		LeftInput:  leftInput,
		RightInput: rightInput,
		Output:     output,
		Operator:   node.Operator,
		Metadata:   node.GetMetadata(),
	}
}
