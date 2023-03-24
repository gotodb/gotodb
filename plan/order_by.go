package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/plan/operator"
)

type OrderByNode struct {
	Input     Node
	Output    Node
	Metadata  *metadata.Metadata
	SortItems []*operator.SortItemNode
	OrderType gtype.OrderType
}

func NewOrderByNode(runtime *config.Runtime, input Node, items []parser.ISortItemContext) *OrderByNode {
	res := &OrderByNode{
		Input:     input,
		Metadata:  metadata.NewMetadata(),
		SortItems: []*operator.SortItemNode{},
	}
	for _, item := range items {
		itemNode := operator.NewSortItemNode(runtime, item)
		res.SortItems = append(res.SortItems, itemNode)
	}
	return res
}

func (n *OrderByNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *OrderByNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *OrderByNode) GetOutput() Node {
	return n.Output
}

func (n *OrderByNode) SetOutput(output Node) {
	n.Output = output
}

func (n *OrderByNode) GetType() NodeType {
	return ORDERBYNODE
}

func (n *OrderByNode) String() string {
	res := "OrderByNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "}\n"
	return res
}

func (n *OrderByNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *OrderByNode) SetMetadata() error {
	if err := n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}
