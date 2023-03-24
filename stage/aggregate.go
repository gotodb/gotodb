package stage

import (
	"github.com/gotodb/gotodb/pb"
)

type AggregateJob struct {
	Location pb.Location
	Inputs   []pb.Location
	Output   pb.Location
}

func (n *AggregateJob) GetType() JobType {
	return JobTypeAggregate
}

func (n *AggregateJob) GetInputs() []pb.Location {
	return n.Inputs
}

func (n *AggregateJob) GetOutputs() []pb.Location {
	return []pb.Location{n.Output}
}

func (n *AggregateJob) GetLocation() pb.Location {
	return n.Location
}

func NewAggregateJob(inputs []pb.Location, output pb.Location) *AggregateJob {
	return &AggregateJob{
		Location: output,
		Inputs:   inputs,
		Output:   output,
	}
}
