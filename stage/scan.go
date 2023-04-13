package stage

import (
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/plan/operator"
)

type ScanJob struct {
	Location      *pb.Location
	Catalog       string
	Schema        string
	Table         string
	Metadata      *metadata.Metadata
	PartitionInfo *partition.Info
	Outputs       []*pb.Location
	Filters       []*operator.BooleanExpressionNode
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

func NewScanJob(node *plan.ScanNode, parInfo *partition.Info, loc *pb.Location, outputs []*pb.Location) *ScanJob {
	parInfo.Encode()
	return &ScanJob{
		Location:      loc,
		Catalog:       node.Catalog,
		Schema:        node.Schema,
		Table:         node.Table,
		Outputs:       outputs,
		Metadata:      node.GetMetadata(),
		PartitionInfo: parInfo,
		Filters:       node.Filters,
	}
}
