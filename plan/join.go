package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/plan/operator"
)

type JoinType int32

const (
	_ JoinType = iota
	LeftJoin
	RightJoin
	InnerJoin
)

type JoinNode struct {
	Metadata              *metadata.Metadata
	LeftInput, RightInput Node
	Output                Node
	JoinType              JoinType
	JoinCriteria          *operator.JoinCriteriaNode
}

func NewJoinNode(_ *config.Runtime, leftInput Node, rightInput Node, joinType JoinType, joinCriteria *operator.JoinCriteriaNode) *JoinNode {
	res := &JoinNode{
		Metadata:     metadata.NewMetadata(),
		LeftInput:    leftInput,
		RightInput:   rightInput,
		JoinType:     joinType,
		JoinCriteria: joinCriteria,
	}
	return res
}

func (n *JoinNode) GetInputs() []Node {
	return []Node{n.LeftInput, n.RightInput}
}

func (n *JoinNode) SetInputs(inputs []Node) {
	n.LeftInput, n.RightInput = inputs[0], inputs[1]
}

func (n *JoinNode) GetOutput() Node {
	return n.Output
}

func (n *JoinNode) SetOutput(output Node) {
	n.Output = output
}

func (n *JoinNode) GetType() NodeType {
	return NodeTypeJoin
}

func (n *JoinNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *JoinNode) SetMetadata() (err error) {
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

func (n *JoinNode) String() string {
	res := "JoinNode {\n"
	res += "LeftInput: " + n.LeftInput.String() + "\n"
	res += "RightInput: " + n.RightInput.String() + "\n"
	res += "JoinType: " + fmt.Sprint(n.JoinType) + "\n"
	res += "JoinCriteria: " + fmt.Sprint(n.JoinCriteria) + "\n"
	res += "}\n"
	return res
}
