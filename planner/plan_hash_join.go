package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type HashJoinPlan struct {
	Metadata              *metadata.Metadata
	LeftInput, RightInput Plan
	Output                Plan
	JoinType              JoinType
	JoinCriteria          *JoinCriteriaNode
	LeftKeys, RightKeys   []*ValueExpressionNode
}

func NewHashJoinNodeFromJoinNode(_ *config.Runtime, node *JoinPlan, leftKeys, rightKeys []*ValueExpressionNode) *HashJoinPlan {
	return &HashJoinPlan{
		Metadata:     node.Metadata,
		LeftInput:    node.LeftInput,
		RightInput:   node.RightInput,
		JoinType:     node.JoinType,
		JoinCriteria: node.JoinCriteria,
		LeftKeys:     leftKeys,
		RightKeys:    rightKeys,
	}
}

func NewHashJoinNode(_ *config.Runtime, leftInput Plan, rightInput Plan, joinType JoinType, joinCriteria *JoinCriteriaNode, leftKeys, rightKeys []*ValueExpressionNode) *HashJoinPlan {
	res := &HashJoinPlan{
		Metadata:     metadata.NewMetadata(),
		LeftInput:    leftInput,
		RightInput:   rightInput,
		JoinType:     joinType,
		JoinCriteria: joinCriteria,
		LeftKeys:     leftKeys,
		RightKeys:    rightKeys,
	}
	return res
}

func (n *HashJoinPlan) GetInputs() []Plan {
	return []Plan{n.LeftInput, n.RightInput}
}

func (n *HashJoinPlan) SetInputs(inputs []Plan) {
	n.LeftInput, n.RightInput = inputs[0], inputs[1]
}

func (n *HashJoinPlan) GetOutput() Plan {
	return n.Output
}

func (n *HashJoinPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *HashJoinPlan) GetType() NodeType {
	return NodeTypeHashJoin
}

func (n *HashJoinPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *HashJoinPlan) SetMetadata() (err error) {
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

func (n *HashJoinPlan) String() string {
	res := "HashJoinPlan {\n"
	res += "LeftInput: " + n.LeftInput.String() + "\n"
	res += "RightInput: " + n.RightInput.String() + "\n"
	res += "JoinType: " + fmt.Sprint(n.JoinType) + "\n"
	res += "JoinCriteria: " + fmt.Sprint(n.JoinCriteria) + "\n"
	res += "LeftKeys: " + fmt.Sprint(n.LeftKeys) + "\n"
	res += "RightKeys: " + fmt.Sprint(n.RightKeys) + "\n"
	res += "}\n"
	return res
}
