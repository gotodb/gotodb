package stage

import (
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
)

type SelectJob struct {
	Location      *pb.Location
	Input, Output *pb.Location
	SetQuantifier *datatype.QuantifierType
	SelectItems   []*planner.SelectItemNode
	Having        *planner.BooleanExpressionNode
	Metadata      *metadata.Metadata
	IsAggregate   bool
}

func (n *SelectJob) GetType() JobType {
	return JobTypeSelect
}

func (n *SelectJob) GetInputs() []*pb.Location {
	return []*pb.Location{n.Input}
}

func (n *SelectJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *SelectJob) GetLocation() *pb.Location {
	return n.Location
}

func NewSelectJob(node *planner.SelectPlan, input, output *pb.Location) *SelectJob {
	return &SelectJob{
		Location:      output,
		Input:         input,
		Output:        output,
		SetQuantifier: node.SetQuantifier,
		SelectItems:   node.SelectItems,
		Having:        node.Having,
		Metadata:      node.GetMetadata(),
		IsAggregate:   node.IsAggregate,
	}
}
