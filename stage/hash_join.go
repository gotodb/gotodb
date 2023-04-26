package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/planner/operator"
)

type HashJoinJob struct {
	Location                *pb.Location
	LeftInputs, RightInputs []*pb.Location
	Output                  *pb.Location
	JoinType                planner.JoinType
	JoinCriteria            *operator.JoinCriteriaNode
	LeftKeys, RightKeys     []*operator.ValueExpressionNode
	Metadata                *metadata.Metadata
}

func (n *HashJoinJob) GetType() JobType {
	return JobTypeHashJoin
}

func (n *HashJoinJob) GetInputs() []*pb.Location {
	var res []*pb.Location
	res = append(res, n.LeftInputs...)
	res = append(res, n.RightInputs...)
	return res
}

func (n *HashJoinJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *HashJoinJob) GetLocation() *pb.Location {
	return n.Location
}

func NewHashJoinJob(node *planner.HashJoinPlan, leftInputs, rightInputs []*pb.Location, output *pb.Location) *HashJoinJob {
	return &HashJoinJob{
		Location:     output,
		LeftInputs:   leftInputs,
		RightInputs:  rightInputs,
		Output:       output,
		JoinType:     node.JoinType,
		JoinCriteria: node.JoinCriteria,
		LeftKeys:     node.LeftKeys,
		RightKeys:    node.RightKeys,
		Metadata:     node.GetMetadata(),
	}
}
