package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
)

type InsertJob struct {
	Location      *pb.Location
	Input, Output *pb.Location
	Catalog       string
	Schema        string
	Table         string
	Columns       []string
	Metadata      *metadata.Metadata
}

func (n *InsertJob) GetType() JobType {
	return JobTypeInsert
}

func (n *InsertJob) GetInputs() []*pb.Location {
	return []*pb.Location{n.Input}
}

func (n *InsertJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *InsertJob) GetLocation() *pb.Location {
	return n.Location
}

func NewInsertJob(node *plan.InsertNode, input, output *pb.Location) *InsertJob {
	return &InsertJob{
		Location: output,
		Input:    input,
		Output:   output,
		Catalog:  node.Catalog,
		Schema:   node.Schema,
		Table:    node.Table,
		Columns:  node.Columns,
		Metadata: node.GetMetadata(),
	}
}
