package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
)

type FilterJob struct {
	Location           *pb.Location
	Inputs             []*pb.Location
	Output             *pb.Location
	BooleanExpressions []*planner.BooleanExpressionNode
	Metadata           *metadata.Metadata
}

func (n *FilterJob) GetType() JobType {
	return JobTypeFilter
}

func (n *FilterJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *FilterJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *FilterJob) GetLocation() *pb.Location {
	return n.Location
}

func NewFilterJob(node *planner.FilterPlan, inputs []*pb.Location, output *pb.Location) *FilterJob {
	return &FilterJob{
		Location:           output,
		Inputs:             inputs,
		Output:             output,
		BooleanExpressions: node.BooleanExpressions,
		Metadata:           node.GetMetadata(),
	}
}
