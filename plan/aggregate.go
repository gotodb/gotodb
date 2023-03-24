package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type AggregateNode struct {
	Input    Node
	Output   Node
	Metadata *metadata.Metadata
}

func NewAggregateNode(_ *config.Runtime, input Node) *AggregateNode {
	return &AggregateNode{
		Input:    input,
		Metadata: metadata.NewMetadata(),
	}
}

func (n *AggregateNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *AggregateNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *AggregateNode) GetOutput() Node {
	return n.Output
}

func (n *AggregateNode) SetOutput(output Node) {
	n.Output = output
}

func (n *AggregateNode) GetType() NodeType {
	return AGGREGATENODE
}

func (n *AggregateNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *AggregateNode) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()

	return nil
}

func (n *AggregateNode) String() string {
	res := "AggregateNode {\n"
	res += n.Input.String()
	res += "}\n"
	return res
}
