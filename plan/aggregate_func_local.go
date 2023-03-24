package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/plan/operator"
)

type AggregateFuncLocalNode struct {
	Input     Node
	Output    Node
	FuncNodes []*operator.FuncCallNode
	Metadata  *metadata.Metadata
}

func NewAggregateFuncLocalNode(_ *config.Runtime, funcs []*operator.FuncCallNode, input Node) *AggregateFuncLocalNode {
	return &AggregateFuncLocalNode{
		Input:     input,
		FuncNodes: funcs,
		Metadata:  metadata.NewMetadata(),
	}
}

func (n *AggregateFuncLocalNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *AggregateFuncLocalNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *AggregateFuncLocalNode) GetOutput() Node {
	return n.Output
}

func (n *AggregateFuncLocalNode) SetOutput(output Node) {
	n.Output = output
}

func (n *AggregateFuncLocalNode) GetType() NodeType {
	return AGGREGATEFUNCLOCALNODE
}

func (n *AggregateFuncLocalNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *AggregateFuncLocalNode) SetMetadata() (err error) {
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

func (n *AggregateFuncLocalNode) String() string {
	res := "AggregateFuncLocalNode {\n"
	res += n.Input.String()
	res += "}\n"
	return res
}
