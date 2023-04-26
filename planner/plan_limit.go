package planner

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type LimitPlan struct {
	Input       Plan
	Output      Plan
	Metadata    *metadata.Metadata
	LimitNumber *int64
}

func NewLimitPlan(_ *config.Runtime, input Plan, t antlr.TerminalNode) *LimitPlan {
	res := &LimitPlan{
		Input:    input,
		Metadata: metadata.NewMetadata(),
	}
	if ns := t.GetText(); ns != "ALL" {
		var num int64
		fmt.Sscanf(ns, "%d", &num)
		res.LimitNumber = &num
	}
	return res
}

func (n *LimitPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *LimitPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *LimitPlan) GetOutput() Plan {
	return n.Output
}

func (n *LimitPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *LimitPlan) GetType() NodeType {
	return NodeTypeLimit
}

func (n *LimitPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *LimitPlan) SetMetadata() error {
	if err := n.Input.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.Input.GetMetadata().Copy()
	return nil
}

func (n *LimitPlan) String() string {
	res := "LimitPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "LimitNubmer: " + fmt.Sprint(*n.LimitNumber) + "\n"
	res += "}\n"
	return res
}
