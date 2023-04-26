package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/planner/operator"
)

type AggregateFuncLocalPlan struct {
	Input     Plan
	Output    Plan
	FuncNodes []*operator.FuncCallNode
	Metadata  *metadata.Metadata
}

func NewAggregateFuncLocalPlan(_ *config.Runtime, funcs []*operator.FuncCallNode, input Plan) *AggregateFuncLocalPlan {
	return &AggregateFuncLocalPlan{
		Input:     input,
		FuncNodes: funcs,
		Metadata:  metadata.NewMetadata(),
	}
}

func (n *AggregateFuncLocalPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *AggregateFuncLocalPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *AggregateFuncLocalPlan) GetOutput() Plan {
	return n.Output
}

func (n *AggregateFuncLocalPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *AggregateFuncLocalPlan) GetType() NodeType {
	return NodeTypeAggregateFuncLocal
}

func (n *AggregateFuncLocalPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *AggregateFuncLocalPlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	for _, f := range n.FuncNodes {
		t, err := f.GetType(n.Input.GetMetadata())
		if err != nil {
			return err
		}
		col := metadata.NewColumnMetadata(t, f.ResColName)
		n.Metadata.AppendColumn(col)
	}

	return nil
}

func (n *AggregateFuncLocalPlan) String() string {
	res := "AggregateFuncLocalPlan {\n"
	res += n.Input.String()
	res += "}\n"
	return res
}
