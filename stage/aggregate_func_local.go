package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/planner/operator"
)

type AggregateFuncLocalJob struct {
	Location  *pb.Location
	Input     *pb.Location
	Output    *pb.Location
	FuncNodes []*operator.FuncCallNode
	Metadata  *metadata.Metadata
}

func (n *AggregateFuncLocalJob) Init(md *metadata.Metadata) error {
	for _, f := range n.FuncNodes {
		if err := f.Init(md); err != nil {
			return err
		}
	}
	return nil
}

func (n *AggregateFuncLocalJob) GetType() JobType {
	return JobTypeAggregateFuncLocal
}

func (n *AggregateFuncLocalJob) GetInputs() []*pb.Location {
	return []*pb.Location{n.Input}
}

func (n *AggregateFuncLocalJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *AggregateFuncLocalJob) GetLocation() *pb.Location {
	return n.Location
}

func NewAggregateFuncLocalJob(node *planner.AggregateFuncLocalPlan, input *pb.Location, output *pb.Location) *AggregateFuncLocalJob {
	res := &AggregateFuncLocalJob{
		Location:  output,
		Input:     input,
		Output:    output,
		FuncNodes: node.FuncNodes,
		Metadata:  node.GetMetadata(),
	}
	return res
}
