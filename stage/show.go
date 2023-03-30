package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
)

// ShowJob show catalogs/schemas/tables/columns/createJob table/createJob view
type ShowJob struct {
	Location    *pb.Location
	Output      *pb.Location
	Metadata    *metadata.Metadata
	ShowType    plan.ShowNodeType
	Catalog     string
	Schema      string
	Table       string
	LikePattern *string
	Escape      *string
}

func (n *ShowJob) GetType() JobType {
	return JobTypeShow
}

func (n *ShowJob) GetInputs() []*pb.Location {
	return []*pb.Location{}
}

func (n *ShowJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *ShowJob) GetLocation() *pb.Location {
	return n.Location
}

func NewShowJob(node *plan.ShowNode, output *pb.Location) *ShowJob {
	return &ShowJob{
		Location:    output,
		Output:      output,
		Metadata:    node.GetMetadata(),
		ShowType:    node.ShowType,
		Catalog:     node.Catalog,
		Schema:      node.Schema,
		Table:       node.Table,
		LikePattern: node.LikePattern,
		Escape:      node.Escape,
	}
}
