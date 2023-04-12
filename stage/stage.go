package stage

import (
	"fmt"
	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/plan/operator"
)

type JobType int32

const (
	_ JobType = iota
	JobTypeScan
	JobTypeSelect
	JobTypeGroupBy
	JobTypeFilter
	JobTypeUnion
	JobTypeLimit
	JobTypeOrderBy
	JobTypeOrderByLocal
	JobTypeJoin
	JobTypeHashJoin
	JobTypeShuffle
	JobTypeHaving
	JobTypeCombine
	JobTypeDuplicate
	JobTypeAggregate
	JobTypeAggregateFuncLocal
	JobTypeAggregateFuncGlobal
	JobTypeBalance
	JobTypeDistinctLocal
	JobTypeDistinctGlobal
	JobTypeShow
)

func (s JobType) String() string {
	switch s {
	case JobTypeScan:
		return "SCAN"
	case JobTypeSelect:
		return "SELECT"
	case JobTypeGroupBy:
		return "GROUP BY"
	case JobTypeFilter:
		return "FILTER"
	case JobTypeUnion:
		return "UNION"
	case JobTypeLimit:
		return "LIMIT"
	case JobTypeOrderBy:
		return "ORDER BY"
	case JobTypeOrderByLocal:
		return "ORDER BY LOCAL"
	case JobTypeJoin:
		return "JOIN"
	case JobTypeHashJoin:
		return "HASH JOIN"
	case JobTypeShuffle:
		return "SHUFFLE"
	case JobTypeHaving:
		return "HAVING"
	case JobTypeCombine:
		return "COMBINE"
	case JobTypeDuplicate:
		return "DUPLICATE"
	case JobTypeAggregate:
		return "AGGREGATE"
	case JobTypeAggregateFuncGlobal:
		return "AGGREGATE FUNC GLOBAL"
	case JobTypeAggregateFuncLocal:
		return "AGGREGATE FUNC LOCAL"
	case JobTypeShow:
		return "SHOW"
	case JobTypeBalance:
		return "BALANCE"
	case JobTypeDistinctLocal:
		return "DISTINCT LOCAL"
	case JobTypeDistinctGlobal:
		return "DISTINCT GLOBAL"
	default:
		return "UNKNOWN"
	}
}

type Job interface {
	GetType() JobType
	GetInputs() []*pb.Location
	GetOutputs() []*pb.Location
	GetLocation() *pb.Location
}

type Worker interface {
	HasExecutor() bool
	GetExecutorLoc() *pb.Location
}

func CreateJob(node plan.Node, executorHeap Worker, pn int) ([]Job, error) {
	if !executorHeap.HasExecutor() {
		return nil, fmt.Errorf("there are no available executor")
	}
	jobs := new([]Job)
	inputJobs, err := createJob(node, jobs, executorHeap, pn)
	if err != nil {
		return nil, err
	}
	output := executorHeap.GetExecutorLoc()

	var inputs []*pb.Location
	for _, inputJob := range inputJobs {
		inputs = append(inputs, inputJob.GetOutputs()...)
	}
	aggJob := NewAggregateJob(inputs, output)
	*jobs = append(*jobs, aggJob)
	return *jobs, err
}

func createJob(inode plan.Node, jobs *[]Job, executorHeap Worker, pn int) ([]Job, error) {
	var res []Job
	switch node := inode.(type) {
	case *plan.ShowNode:
		output := executorHeap.GetExecutorLoc()
		output.ChannelIndex = int32(0)
		res = append(res, NewShowJob(node, output))
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.ScanNode:
		var outputs []*pb.Location
		for i := 0; i < pn; i++ {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = int32(0)
			outputs = append(outputs, output)
		}
		scanNodePar, err := node.Connector.GetPartitionInfo(pn)
		if err != nil {
			return res, err
		}

		parInfos := make([]*partition.Info, pn)
		recMap := make([]map[int]int, pn)
		for i := 0; i < pn; i++ {
			parInfos[i] = partition.New(scanNodePar.Metadata)
			recMap[i] = map[int]int{}
		}

		k := 0
		if scanNodePar.IsPartition() {
			partitionNum := scanNodePar.GetPartitionNum()
			var parFilters []*operator.BooleanExpressionNode
			for _, f := range node.Filters {
				cols, err := f.GetColumns()
				if err != nil {
					return res, err
				}
				if scanNodePar.Metadata.Contains(cols) {
					parFilters = append(parFilters, f)
				}
			}

			for i := 0; i < partitionNum; i++ {
				prg := scanNodePar.GetPartitionRowGroup(i)
				flag := true
				for _, exp := range parFilters {
					if r, err := exp.Result(prg); err != nil {
						return res, err
					} else if !r.([]interface{})[0].(bool) {
						flag = false
						break
					}
				}
				if !flag {
					continue
				}

				row, _ := prg.Read()
				location := scanNodePar.GetLocation(i)
				fileType := scanNodePar.GetFileType(i)
				files := scanNodePar.GetPartitionFiles(i)
				for _, file := range files {
					if _, ok := recMap[k][i]; !ok {
						recMap[k][i] = parInfos[k].GetPartitionNum()
						parInfos[k].Write(row)
						parInfos[k].Locations = append(parInfos[k].Locations, location)
						parInfos[k].FileTypes = append(parInfos[k].FileTypes, fileType)
						parInfos[k].FileLists = append(parInfos[k].FileLists, []*filesystem.FileLocation{})
					}
					j := recMap[k][i]
					parInfos[k].FileLists[j] = append(parInfos[k].FileLists[j], file)

					k++
					k = k % pn
				}
			}
		} else {
			for i, file := range scanNodePar.GetNoPartitionFiles() {
				parInfos[i%pn].FileList = append(parInfos[i%pn].FileList, file)
			}
		}

		var resScan []Job
		for i := 0; i < pn; i++ {
			resScan = append(resScan, NewScanJob(node, parInfos[i], outputs[i], []*pb.Location{outputs[i]}))
		}

		*jobs = append(*jobs, resScan...)
		return resScan, nil

	case *plan.SelectNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		if node.SetQuantifier == nil || (*node.SetQuantifier != gtype.DISTINCT) || len(inputJobs) == 1 {
			var inputs []*pb.Location
			for _, inputNode := range inputJobs {
				inputs = append(inputs, inputNode.GetOutputs()...)
			}
			ln := len(inputs)
			if ln > 1 {
				for i := 0; i < ln; i++ {
					output := executorHeap.GetExecutorLoc()
					output.ChannelIndex = 0
					input := inputs[i]
					res = append(res, NewSelectJob(node, input, output))
				}
			} else {
				nodeLoc := executorHeap.GetExecutorLoc()
				var outputs []*pb.Location
				for i := 0; i < pn; i++ {
					nodeLoc.ChannelIndex = int32(i)
					outputs = append(outputs, nodeLoc)

					selLoc := executorHeap.GetExecutorLoc()
					selLoc.ChannelIndex = 0
					res = append(res, NewSelectJob(node, nodeLoc, selLoc))
				}
				*jobs = append(*jobs, NewBalanceJob(inputs, outputs))
			}

			*jobs = append(*jobs, res...)

		} else {
			//for select distinct
			aggLoc := executorHeap.GetExecutorLoc()
			aggLoc.ChannelIndex = 0
			var inputLocs []*pb.Location
			for _, inputNode := range inputJobs {
				inputLocs = append(inputLocs, inputNode.GetOutputs()...)
			}
			aggregateJob := NewAggregateJob(inputLocs, aggLoc)
			selectLoc := executorHeap.GetExecutorLoc()
			selectLoc.ChannelIndex = 0
			newSelectJob := NewSelectJob(node, aggLoc, selectLoc)
			res = append(res, newSelectJob)
			*jobs = append(*jobs, aggregateJob, newSelectJob)
		}
		return res, nil

	case *plan.GroupByNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				output.ChannelIndex = 0
				res = append(res, NewGroupByJob(node, input, output))
			}
		}

		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.JoinNode:
		leftInputJobs, err1 := createJob(node.LeftInput, jobs, executorHeap, pn)
		if err1 != nil {
			return nil, err1
		}
		rightInputJobs, err2 := createJob(node.RightInput, jobs, executorHeap, pn)
		if err2 != nil {
			return nil, err2
		}

		//duplicate right inputs
		var inputs []*pb.Location
		var outputs []*pb.Location
		for _, inputJob := range rightInputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}
		output := executorHeap.GetExecutorLoc()
		for i := 0; i < pn; i++ {
			output.ChannelIndex = int32(i)
			outputs = append(outputs, output)
		}
		duplicateJob := NewDuplicateJob(inputs, outputs, nil)

		//join
		rightInputs := duplicateJob.GetOutputs()
		var leftInputs []*pb.Location
		for _, leftInputJob := range leftInputJobs {
			leftInputs = append(leftInputs, leftInputJob.GetOutputs()...)
		}
		if len(leftInputs) != len(rightInputs) {
			return nil, fmt.Errorf("join job leftInputs number != rightInputs number")
		}

		for i := 0; i < len(leftInputs); i++ {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = 0
			res = append(res, NewJoinJob(node, leftInputs[i], rightInputs[i], output))
		}
		*jobs = append(*jobs, duplicateJob)
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.HashJoinNode:
		leftInputJobs, err1 := createJob(node.LeftInput, jobs, executorHeap, pn)
		if err1 != nil {
			return nil, err1
		}
		rightInputJobs, err2 := createJob(node.RightInput, jobs, executorHeap, pn)
		if err2 != nil {
			return nil, err2
		}

		//shuffle left inputs
		var leftInputs []*pb.Location
		var leftShuffleJobs []Job
		for _, inputJob := range leftInputJobs {
			leftInputs = append(leftInputs, inputJob.GetOutputs()...)
		}
		for _, input := range leftInputs {
			var outputs []*pb.Location
			output := executorHeap.GetExecutorLoc()

			for i := 0; i < pn; i++ {
				output.ChannelIndex = int32(i)
				outputs = append(outputs, output)
			}

			var keyExps []*operator.ExpressionNode
			for _, key := range node.LeftKeys {
				exp := &operator.ExpressionNode{
					BooleanExpression: &operator.BooleanExpressionNode{
						Predicated: &operator.PredicatedNode{
							ValueExpression: key,
						},
					},
				}
				keyExps = append(keyExps, exp)
			}
			leftShuffleJobs = append(leftShuffleJobs, NewShuffleNode([]*pb.Location{input}, outputs, keyExps))
		}

		//shuffle right inputs
		var rightInputs []*pb.Location
		var rightShuffleJobs []Job
		for _, inputJob := range rightInputJobs {
			rightInputs = append(rightInputs, inputJob.GetOutputs()...)
		}
		for _, input := range rightInputs {
			var outputs []*pb.Location
			output := executorHeap.GetExecutorLoc()
			for i := 0; i < pn; i++ {
				output.ChannelIndex = int32(i)
				outputs = append(outputs, output)
			}
			var keyExps []*operator.ExpressionNode
			for _, key := range node.RightKeys {
				exp := &operator.ExpressionNode{
					BooleanExpression: &operator.BooleanExpressionNode{
						Predicated: &operator.PredicatedNode{
							ValueExpression: key,
						},
					},
				}
				keyExps = append(keyExps, exp)
			}
			rightShuffleJobs = append(rightShuffleJobs, NewShuffleNode([]*pb.Location{input}, outputs, keyExps))
		}

		leftJoinInputs, rightJoinInputs := make([][]*pb.Location, pn), make([][]*pb.Location, pn)
		for _, job := range leftShuffleJobs {
			outputs := job.GetOutputs()
			for i, output := range outputs {
				leftJoinInputs[i] = append(leftJoinInputs[i], output)
			}
		}

		for _, job := range rightShuffleJobs {
			outputs := job.GetOutputs()
			for i, output := range outputs {
				rightJoinInputs[i] = append(rightJoinInputs[i], output)
			}
		}

		//hash join
		for i := 0; i < pn; i++ {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = 0
			res = append(res, NewHashJoinJob(node, leftJoinInputs[i], rightJoinInputs[i], output))
		}
		*jobs = append(*jobs, leftShuffleJobs...)
		*jobs = append(*jobs, rightShuffleJobs...)
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.LimitNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}

		limitNodeLoc := executorHeap.GetExecutorLoc()
		limitNodeLoc.ChannelIndex = 0
		res = append(res, NewLimitJob(node, inputs, limitNodeLoc))

		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.DistinctLocalNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}

		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}

		for i := 0; i < len(inputs); i++ {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = int32(0)
			res = append(res, NewDistinctLocalJob(node, []*pb.Location{inputs[i]}, []*pb.Location{output}))
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.DistinctGlobalNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}

		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}

		loc := executorHeap.GetExecutorLoc()

		var outputs []*pb.Location
		for i := 0; i < len(inputs); i++ {
			loc.ChannelIndex = int32(i)
			outputs = append(outputs, loc)
		}
		distGlobalJob := NewDistinctGlobalJob(node, inputs, outputs)

		res = append(res, distGlobalJob)
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.AggregateNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		output := executorHeap.GetExecutorLoc()
		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}
		res = append(res, NewAggregateJob(inputs, output))
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.AggregateFuncGlobalNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		output := executorHeap.GetExecutorLoc()
		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}
		res = append(res, NewAggregateFuncGlobalJob(node, inputs, output))
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.AggregateFuncLocalNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				output.ChannelIndex = 0
				res = append(res, NewAggregateFuncLocalJob(node, input, output))
			}
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.FilterNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				output.ChannelIndex = 0
				res = append(res, NewFilterJob(node, input, output))
			}
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.UnionNode:
		leftInputJobs, err1 := createJob(node.LeftInput, jobs, executorHeap, pn)
		if err1 != nil {
			return nil, err1
		}
		rightInputJobs, err2 := createJob(node.RightInput, jobs, executorHeap, pn)
		if err2 != nil {
			return nil, err2
		}

		//union
		var leftInputs []*pb.Location
		for _, leftInputJob := range leftInputJobs {
			leftInputs = append(leftInputs, leftInputJob.GetOutputs()...)
		}
		var rightInputs []*pb.Location
		for _, rightInputJob := range rightInputJobs {
			rightInputs = append(rightInputs, rightInputJob.GetOutputs()...)
		}

		if len(leftInputs) != len(rightInputs) {
			return nil, fmt.Errorf("join job leftInputs number != rightInputs number")
		}

		for i := 0; i < len(leftInputs); i++ {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = 0
			res = append(res, NewUnionJob(node, leftInputs[i], rightInputs[i], output))
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.OrderByNode:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return nil, err
		}

		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}

		var localRes []Job
		for _, input := range inputs {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = 0
			localRes = append(localRes, NewOrderByLocalJob(node, input, output))
		}

		inputs = []*pb.Location{}
		for _, inputJob := range localRes {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}
		output := executorHeap.GetExecutorLoc()
		newOrderByJob := NewOrderByJob(node, inputs, output)
		res = append(res, newOrderByJob)

		*jobs = append(*jobs, localRes...)
		*jobs = append(*jobs, newOrderByJob)
		return res, nil

	default:
		logger.Errorf("create job: unknown type")
		return nil, fmt.Errorf("create job: unknown type")

	}
}
