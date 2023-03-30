package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
)

type LimitJob struct {
	Location    *pb.Location
	Inputs      []*pb.Location
	Output      *pb.Location
	LimitNumber *int64
	Metadata    *metadata.Metadata
}

func (n *LimitJob) GetType() JobType {
	return JobTypeLimit
}

func (n *LimitJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *LimitJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *LimitJob) GetLocation() *pb.Location {
	return n.Location
}

func NewLimitJob(node *plan.LimitNode, inputs []*pb.Location, output *pb.Location) *LimitJob {
	return &LimitJob{
		Location:    output,
		Inputs:      inputs,
		Output:      output,
		LimitNumber: node.LimitNumber,
		Metadata:    node.GetMetadata(),
	}
}
