package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/planner/operator"
)

type OrderByLocalJob struct {
	Location  *pb.Location
	Input     *pb.Location
	Output    *pb.Location
	SortItems []*operator.SortItemNode
	Metadata  *metadata.Metadata
}

func (n *OrderByLocalJob) GetType() JobType {
	return JobTypeOrderByLocal
}

func (n *OrderByLocalJob) GetInputs() []*pb.Location {
	return []*pb.Location{n.Input}
}

func (n *OrderByLocalJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *OrderByLocalJob) GetLocation() *pb.Location {
	return n.Location
}

func NewOrderByLocalJob(node *planner.OrderByPlan, input *pb.Location, output *pb.Location) *OrderByLocalJob {
	return &OrderByLocalJob{
		Location:  output,
		Input:     input,
		Output:    output,
		SortItems: node.SortItems,
		Metadata:  node.GetMetadata(),
	}
}
