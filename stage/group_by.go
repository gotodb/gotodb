package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/plan/operator"
)

type GroupByJob struct {
	Location      pb.Location
	Input, Output pb.Location
	GroupBy       *operator.GroupByNode
	Metadata      *metadata.Metadata
}

func (n *GroupByJob) GetType() JobType {
	return JobTypeGroupBy
}

func (n *GroupByJob) GetInputs() []pb.Location {
	return []pb.Location{n.Input}
}

func (n *GroupByJob) GetOutputs() []pb.Location {
	return []pb.Location{n.Output}
}

func (n *GroupByJob) GetLocation() pb.Location {
	return n.Location
}

func NewGroupByJob(node *plan.GroupByNode, input, output pb.Location) *GroupByJob {
	return &GroupByJob{
		Location: output,
		Input:    input,
		Output:   output,
		GroupBy:  node.GroupBy,
		Metadata: node.GetMetadata(),
	}
}
