package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type HashJoinNode struct {
	Metadata              *metadata.Metadata
	LeftInput, RightInput Node
	Output                Node
	JoinType              JoinType
	JoinCriteria          *operator.JoinCriteriaNode
	LeftKeys, RightKeys   []*operator.ValueExpressionNode
}

func NewHashJoinNodeFromJoinNode(_ *config.Runtime, node *JoinNode, leftKeys, rightKeys []*operator.ValueExpressionNode) *HashJoinNode {
	return &HashJoinNode{
		Metadata:     node.Metadata,
		LeftInput:    node.LeftInput,
		RightInput:   node.RightInput,
		JoinType:     node.JoinType,
		JoinCriteria: node.JoinCriteria,
		LeftKeys:     leftKeys,
		RightKeys:    rightKeys,
	}
}

func NewHashJoinNode(_ *config.Runtime, leftInput Node, rightInput Node, joinType JoinType, joinCriteria *operator.JoinCriteriaNode, leftKeys, rightKeys []*operator.ValueExpressionNode) *HashJoinNode {
	res := &HashJoinNode{
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

func (n *HashJoinNode) GetInputs() []Node {
	return []Node{n.LeftInput, n.RightInput}
}

func (n *HashJoinNode) SetInputs(inputs []Node) {
	n.LeftInput, n.RightInput = inputs[0], inputs[1]
}

func (n *HashJoinNode) GetOutput() Node {
	return n.Output
}

func (n *HashJoinNode) SetOutput(output Node) {
	n.Output = output
}

func (n *HashJoinNode) GetType() NodeType {
	return HASHJOINNODE
}

func (n *HashJoinNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *HashJoinNode) SetMetadata() (err error) {
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

func (n *HashJoinNode) String() string {
	res := "HashJoinNode {\n"
	res += "LeftInput: " + n.LeftInput.String() + "\n"
	res += "RightInput: " + n.RightInput.String() + "\n"
	res += "JoinType: " + fmt.Sprint(n.JoinType) + "\n"
	res += "JoinCriteria: " + fmt.Sprint(n.JoinCriteria) + "\n"
	res += "LeftKeys: " + fmt.Sprint(n.LeftKeys) + "\n"
	res += "RightKeys: " + fmt.Sprint(n.RightKeys) + "\n"
	res += "}\n"
	return res
}
