package stage

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/plan/operator"
)

type JoinJob struct {
	Location              *pb.Location
	LeftInput, RightInput *pb.Location
	Output                *pb.Location
	JoinType              plan.JoinType
	JoinCriteria          *operator.JoinCriteriaNode
	Metadata              *metadata.Metadata
}

func (n *JoinJob) GetType() JobType {
	return JobTypeJoin
}

func (n *JoinJob) GetInputs() []*pb.Location {
	return []*pb.Location{n.LeftInput, n.RightInput}
}

func (n *JoinJob) GetOutputs() []*pb.Location {
	return []*pb.Location{n.Output}
}

func (n *JoinJob) GetLocation() *pb.Location {
	return n.Location
}

func NewJoinJob(node *plan.JoinNode, leftInput, rightInput *pb.Location, output *pb.Location) *JoinJob {
	return &JoinJob{
		Location:     output,
		LeftInput:    leftInput,
		RightInput:   rightInput,
		Output:       output,
		JoinType:     node.JoinType,
		JoinCriteria: node.JoinCriteria,
		Metadata:     node.GetMetadata(),
	}
}
