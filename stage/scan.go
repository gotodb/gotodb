package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/partition"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/planner/operator"
)

type ScanJob struct {
	Location  *pb.Location
	Catalog   string
	Schema    string
	Table     string
	Metadata  *metadata.Metadata
	Partition *partition.Partition
	Outputs   []*pb.Location
	Filters   []*operator.BooleanExpressionNode
}

func (n *ScanJob) GetType() JobType {
	return JobTypeScan
}

func (n *ScanJob) GetInputs() []*pb.Location {
	return []*pb.Location{}
}

func (n *ScanJob) GetOutputs() []*pb.Location {
	return n.Outputs
}

func (n *ScanJob) GetLocation() *pb.Location {
	return n.Location
}

func NewScanJob(node *planner.ScanPlan, par *partition.Partition, loc *pb.Location, outputs []*pb.Location) *ScanJob {
	par.Encode()
	return &ScanJob{
		Location:  loc,
		Catalog:   node.Catalog,
		Schema:    node.Schema,
		Table:     node.Table,
		Outputs:   outputs,
		Metadata:  node.GetMetadata(),
		Partition: par,
		Filters:   node.Filters,
	}
}
