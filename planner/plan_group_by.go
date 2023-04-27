package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
)

type GroupByPlan struct {
	Input    Plan
	Output   Plan
	Metadata *metadata.Metadata
	GroupBy  *GroupByNode
}

func NewGroupByPlan(runtime *config.Runtime, input Plan, groupBy parser.IGroupByContext) *GroupByPlan {
	return &GroupByPlan{
		Input:    input,
		Metadata: metadata.NewMetadata(),
		GroupBy:  NewGroupByNode(runtime, groupBy),
	}
}

func (n *GroupByPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *GroupByPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *GroupByPlan) GetOutput() Plan {
	return n.Output
}

func (n *GroupByPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *GroupByPlan) GetType() NodeType {
	return NodeTypeGroupBy
}

func (n *GroupByPlan) SetMetadata() (err error) {
	if err = n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	n.Metadata.AppendKeyByType(datatype.STRING)
	return nil
}

func (n *GroupByPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *GroupByPlan) String() string {
	res := "GroupByPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "GroupBy: " + fmt.Sprint(n.GroupBy) + "\n"
	res += "}/n"
	return res
}
