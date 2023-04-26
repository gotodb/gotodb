package planner

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type UnionType int32

const (
	_ UnionType = iota
	INTERSECT
	UNION
	EXCEPT
)

type UnionPlan struct {
	LeftInput  Plan
	RightInput Plan
	Output     Plan
	Operator   UnionType
	Metadata   *metadata.Metadata
}

func NewUnionPlan(_ *config.Runtime, left, right Plan, op antlr.Token) *UnionPlan {
	var operator UnionType
	switch op.GetText() {
	case "INTERSECT":
		operator = INTERSECT
	case "UNION":
		operator = UNION
	case "EXCEPT":
		operator = EXCEPT
	}

	res := &UnionPlan{
		LeftInput:  left,
		RightInput: right,
		Operator:   operator,
		Metadata:   metadata.NewMetadata(),
	}
	return res
}

func (n *UnionPlan) GetInputs() []Plan {
	return []Plan{n.LeftInput, n.RightInput}
}

func (n *UnionPlan) SetInputs(inputs []Plan) {
	n.LeftInput, n.RightInput = inputs[0], inputs[1]
}

func (n *UnionPlan) GetOutput() Plan {
	return n.Output
}

func (n *UnionPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *UnionPlan) GetType() NodeType {
	return NodeTypeUnion
}

func (n *UnionPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *UnionPlan) SetMetadata() (err error) {
	if err = n.LeftInput.SetMetadata(); err != nil {
		return err
	}
	if err = n.RightInput.SetMetadata(); err != nil {
		return err
	}
	n.Metadata = n.LeftInput.GetMetadata().Copy()
	return nil
}

func (n *UnionPlan) String() string {
	res := "UnionPlan {\n"
	res += "LeftInput: " + n.LeftInput.String() + "\n"
	res += "RightInput: " + n.RightInput.String() + "\n"
	res += "Operator: " + fmt.Sprint(n.Operator) + "\n"
	res += "}\n"
	return res
}
