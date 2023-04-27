package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
)

type FilterJob struct {
	Location           *pb.Location
	Input, Output      *pb.Location
	BooleanExpressions []*planner.BooleanExpressionNode
	Metadata           *metadata.Metadata
}

func (n *FilterJob) GetType() JobType {
	return JobTypeFilter
}

func (n *FilterJob) GetInputs() []*pb.Location {
	return []*pb.Location{n.Input}
}

func (n *FilterJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *FilterJob) GetLocation() *pb.Location {
	return n.Location
}

func NewFilterJob(node *planner.FilterPlan, input, output *pb.Location) *FilterJob {
	return &FilterJob{
		Location:           output,
		Input:              input,
		Output:             output,
		BooleanExpressions: node.BooleanExpressions,
		Metadata:           node.GetMetadata(),
	}
}
