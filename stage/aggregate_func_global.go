package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/plan/operator"
)

type AggregateFuncGlobalJob struct {
	Location  pb.Location
	Inputs    []pb.Location
	Output    pb.Location
	FuncNodes []*operator.FuncCallNode
	Metadata  *metadata.Metadata
}

func (n *AggregateFuncGlobalJob) Init(md *metadata.Metadata) error {
	for _, f := range n.FuncNodes {
		if err := f.Init(md); err != nil {
			return err
		}
	}
	return nil
}

func (n *AggregateFuncGlobalJob) GetType() JobType {
	return JobTypeAggregateFuncGlobal
}

func (n *AggregateFuncGlobalJob) GetInputs() []pb.Location {
	return n.Inputs
}

func (n *AggregateFuncGlobalJob) GetOutputs() []pb.Location {
	return []pb.Location{n.Output}
}

func (n *AggregateFuncGlobalJob) GetLocation() pb.Location {
	return n.Location
}

func NewAggregateFuncGlobalJob(node *plan.AggregateFuncGlobalNode, inputs []pb.Location, output pb.Location) *AggregateFuncGlobalJob {
	res := &AggregateFuncGlobalJob{
		Location:  output,
		Inputs:    inputs,
		Output:    output,
		FuncNodes: node.FuncNodes,
		Metadata:  node.GetMetadata(),
	}
	return res
}
