package stage

import (
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
)

type ShuffleJob struct {
	Location        *pb.Location
	Keys            []*planner.ExpressionNode
	Inputs, Outputs []*pb.Location
}

func (n *ShuffleJob) GetType() JobType {
	return JobTypeShuffle
}

func (n *ShuffleJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *ShuffleJob) GetOutputs() []*pb.Location {
	return n.Outputs
}

func (n *ShuffleJob) GetLocation() *pb.Location {
	return n.Location
}

func NewShuffleNode(inputs, outputs []*pb.Location, keys []*planner.ExpressionNode) *ShuffleJob {
	return &ShuffleJob{
		Location: outputs[0],
		Keys:     keys,
		Inputs:   inputs,
		Outputs:  outputs,
	}
}
