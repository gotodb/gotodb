package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/planner/operator"
)

type DistinctLocalJob struct {
	Location    *pb.Location
	Inputs      []*pb.Location
	Outputs     []*pb.Location
	Expressions []*operator.ExpressionNode
	Metadata    *metadata.Metadata
}

func (n *DistinctLocalJob) Init(md *metadata.Metadata) error {
	for _, e := range n.Expressions {
		if err := e.Init(md); err != nil {
			return err
		}
	}
	return nil
}

func (n *DistinctLocalJob) GetType() JobType {
	return JobTypeDistinctLocal
}

func (n *DistinctLocalJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *DistinctLocalJob) GetOutputs() []*pb.Location {
	return n.Outputs
}

func (n *DistinctLocalJob) GetLocation() *pb.Location {
	return n.Location
}

func NewDistinctLocalJob(node *planner.DistinctLocalPlan, inputs []*pb.Location, outputs []*pb.Location) *DistinctLocalJob {
	res := &DistinctLocalJob{
		Location:    outputs[0],
		Inputs:      inputs,
		Outputs:     outputs,
		Expressions: node.Expressions,
		Metadata:    node.GetMetadata(),
	}
	return res
}
