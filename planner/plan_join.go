package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type JoinType int32

const (
	_ JoinType = iota
	LeftJoin
	RightJoin
	InnerJoin
)

type JoinPlan struct {
	Metadata              *metadata.Metadata
	LeftInput, RightInput Plan
	Output                Plan
	JoinType              JoinType
	JoinCriteria          *JoinCriteriaNode
}

func NewJoinPlan(_ *config.Runtime, leftInput Plan, rightInput Plan, joinType JoinType, joinCriteria *JoinCriteriaNode) *JoinPlan {
	res := &JoinPlan{
		Metadata:     metadata.NewMetadata(),
		LeftInput:    leftInput,
		RightInput:   rightInput,
		JoinType:     joinType,
		JoinCriteria: joinCriteria,
	}
	return res
}

func (n *JoinPlan) GetInputs() []Plan {
	return []Plan{n.LeftInput, n.RightInput}
}

func (n *JoinPlan) SetInputs(inputs []Plan) {
	n.LeftInput, n.RightInput = inputs[0], inputs[1]
}

func (n *JoinPlan) GetOutput() Plan {
	return n.Output
}

func (n *JoinPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *JoinPlan) GetType() NodeType {
	return NodeTypeJoin
}

func (n *JoinPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *JoinPlan) SetMetadata() (err error) {
	if err = n.LeftInput.SetMetadata(); err != nil {
		return err
	}
	if err = n.RightInput.SetMetadata(); err != nil {
		return err
	}

	mdl, mdr := n.LeftInput.GetMetadata(), n.RightInput.GetMetadata()
	n.Metadata = metadata.JoinMetadata(mdl, mdr)
	return nil
}

func (n *JoinPlan) String() string {
	res := "JoinPlan {\n"
	res += "LeftInput: " + n.LeftInput.String() + "\n"
	res += "RightInput: " + n.RightInput.String() + "\n"
	res += "JoinType: " + fmt.Sprint(n.JoinType) + "\n"
	res += "JoinCriteria: " + fmt.Sprint(n.JoinCriteria) + "\n"
	res += "}\n"
	return res
}
