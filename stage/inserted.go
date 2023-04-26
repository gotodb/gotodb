package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
)

type InsertedJob struct {
	Location *pb.Location
	Inputs   []*pb.Location
	Output   *pb.Location
	Metadata *metadata.Metadata
}

func (n *InsertedJob) GetType() JobType {
	return JobTypeInserted
}

func (n *InsertedJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *InsertedJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *InsertedJob) GetLocation() *pb.Location {
	return n.Location
}

func NewInsertedJob(node *planner.InsertPlan, inputs []*pb.Location, output *pb.Location) *InsertedJob {
	return &InsertedJob{
		Location: output,
		Inputs:   inputs,
		Output:   output,
		Metadata: node.GetMetadata(),
	}
}
