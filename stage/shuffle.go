package stage

import (
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan/operator"
)

type ShuffleJob struct {
	Location        *pb.Location
	Keys            []*operator.ExpressionNode
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

func NewShuffleNode(inputs, outputs []*pb.Location, keys []*operator.ExpressionNode) *ShuffleJob {
	return &ShuffleJob{
		Location: outputs[0],
		Keys:     keys,
		Inputs:   inputs,
		Outputs:  outputs,
	}
}
