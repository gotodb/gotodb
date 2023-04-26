package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/planner/operator"
)

type OrderByPlan struct {
	Input     Plan
	Output    Plan
	Metadata  *metadata.Metadata
	SortItems []*operator.SortItemNode
	OrderType datatype.OrderType
}

func NewOrderByPlan(runtime *config.Runtime, input Plan, items []parser.ISortItemContext) *OrderByPlan {
	res := &OrderByPlan{
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

func (n *OrderByPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *OrderByPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *OrderByPlan) GetOutput() Plan {
	return n.Output
}

func (n *OrderByPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *OrderByPlan) GetType() NodeType {
	return NodeTypeOrderBy
}

func (n *OrderByPlan) String() string {
	res := "OrderByPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "}\n"
	return res
}

func (n *OrderByPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *OrderByPlan) SetMetadata() error {
	if err := n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}
