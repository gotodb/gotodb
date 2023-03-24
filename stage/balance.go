package stage

import (
	"github.com/gotodb/gotodb/pb"
)

type BalanceJob struct {
	Location        pb.Location
	Inputs, Outputs []pb.Location
}

func (n *BalanceJob) GetType() JobType {
	return JobTypeBalance
}

func (n *BalanceJob) GetInputs() []pb.Location {
	return n.Inputs
}

func (n *BalanceJob) GetOutputs() []pb.Location {
	return n.Outputs
}

func (n *BalanceJob) GetLocation() pb.Location {
	return n.Location
}

func NewBalanceJob(inputs, outputs []pb.Location) *BalanceJob {
	return &BalanceJob{
		Location: outputs[0],
		Inputs:   inputs,
		Outputs:  outputs,
	}
}
