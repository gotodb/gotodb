package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
)

type DistinctGlobalJob struct {
	Location    *pb.Location
	Inputs      []*pb.Location
	Outputs     []*pb.Location
	Expressions []*planner.ExpressionNode
	Metadata    *metadata.Metadata
}

func (n *DistinctGlobalJob) Init(md *metadata.Metadata) error {
	for _, e := range n.Expressions {
		if err := e.Init(md); err != nil {
			return err
		}
	}
	return nil
}

func (n *DistinctGlobalJob) GetType() JobType {
	return JobTypeDistinctGlobal
}

func (n *DistinctGlobalJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *DistinctGlobalJob) GetOutputs() []*pb.Location {
	return n.Outputs
}

func (n *DistinctGlobalJob) GetLocation() *pb.Location {
	return n.Location
}

func NewDistinctGlobalJob(node *planner.DistinctGlobalPlan, inputs []*pb.Location, outputs []*pb.Location) *DistinctGlobalJob {
	res := &DistinctGlobalJob{
		Location:    outputs[0],
		Inputs:      inputs,
		Outputs:     outputs,
		Expressions: node.Expressions,
		Metadata:    node.GetMetadata(),
	}
	return res
}
