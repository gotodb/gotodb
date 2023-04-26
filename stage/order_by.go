package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/planner/operator"
)

type OrderByJob struct {
	Location  *pb.Location
	Inputs    []*pb.Location
	Output    *pb.Location
	SortItems []*operator.SortItemNode
	Metadata  *metadata.Metadata
}

func (n *OrderByJob) GetType() JobType {
	return JobTypeOrderBy
}

func (n *OrderByJob) GetInputs() []*pb.Location {
	return n.Inputs
}

func (n *OrderByJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *OrderByJob) GetLocation() *pb.Location {
	return n.Location
}

func NewOrderByJob(node *planner.OrderByPlan, inputs []*pb.Location, output *pb.Location) *OrderByJob {
	return &OrderByJob{
		Location:  output,
		Inputs:    inputs,
		Output:    output,
		SortItems: node.SortItems,
		Metadata:  node.GetMetadata(),
	}
}
