package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/plan/operator"
)

type AggregateFuncGlobalNode struct {
	Input     Node
	Output    Node
	FuncNodes []*operator.FuncCallNode
	Metadata  *metadata.Metadata
}

func NewAggregateFuncGlobalNode(_ *config.Runtime, funcs []*operator.FuncCallNode, input Node) *AggregateFuncGlobalNode {
	return &AggregateFuncGlobalNode{
		Input:     input,
		FuncNodes: funcs,
		Metadata:  metadata.NewMetadata(),
	}
}

func (n *AggregateFuncGlobalNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *AggregateFuncGlobalNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *AggregateFuncGlobalNode) GetOutput() Node {
	return n.Output
}

func (n *AggregateFuncGlobalNode) SetOutput(output Node) {
	n.Output = output
}

func (n *AggregateFuncGlobalNode) GetType() NodeType {
	return AGGREGATEFUNCGLOBALNODE
}

func (n *AggregateFuncGlobalNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *AggregateFuncGlobalNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *AggregateFuncGlobalNode) String() string {
	res := "AggregateFuncGlobalNode {\n"
	res += n.Input.String()
	res += "}\n"
	return res
}
