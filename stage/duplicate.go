package stage

import (
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner/operator"
)

type DuplicateJob struct {
	Location        *pb.Location
	Keys            []*operator.ValueExpressionNode
	Inputs, Outputs []*pb.Location
}

func (n *DuplicateJob) GetType() JobType {
	return JobTypeDuplicate
}

func (n *DuplicateJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *DuplicateJob) GetOutputs() []*pb.Location {
	return n.Outputs
}

func (n *DuplicateJob) GetLocation() *pb.Location {
	return n.Location
}

func NewDuplicateJob(inputs, outputs []*pb.Location, keys []*operator.ValueExpressionNode) *DuplicateJob {
	return &DuplicateJob{
		Location: outputs[0],
		Keys:     keys,
		Inputs:   inputs,
		Outputs:  outputs,
	}
}
