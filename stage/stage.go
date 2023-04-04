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

func CreateJob(node plan.Node, jobs *[]Job, executorHeap Worker, pn int) (Job, error) {
	if !executorHeap.HasExecutor() {
		return nil, fmt.Errorf("there are no available executor")
	}
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
	return aggJob, err
}

func createJob(node plan.Node, jobs *[]Job, executorHeap Worker, pn int) ([]Job, error) {
	var res []Job
	switch node.(type) {
	case *plan.ShowNode:
		showNode := node.(*plan.ShowNode)
		output := executorHeap.GetExecutorLoc()
		output.ChannelIndex = int32(0)
		res = append(res, NewShowJob(showNode, output))
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.ScanNode:
		scanNode := node.(*plan.ScanNode)
		var outputs []*pb.Location
		for i := 0; i < pn; i++ {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = int32(0)
			outputs = append(outputs, output)
		}

		parInfos := make([]*partition.Info, pn)
		recMap := make([]map[int]int, pn)
		for i := 0; i < pn; i++ {
			parInfos[i] = partition.NewInfo(scanNode.PartitionInfo.Metadata)
			recMap[i] = map[int]int{}
		}

		k := 0
		if scanNode.PartitionInfo.IsPartition() {
			partitionNum := scanNode.PartitionInfo.GetPartitionNum()
			var parFilters []*operator.BooleanExpressionNode
			for _, f := range scanNode.Filters {
				cols, err := f.GetColumns()
				if err != nil {
					return res, err
				}
				if scanNode.PartitionInfo.Metadata.Contains(cols) {
					parFilters = append(parFilters, f)
				}
			}

			for i := 0; i < partitionNum; i++ {
				prg := scanNode.PartitionInfo.GetPartitionRowGroup(i)
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
				location := scanNode.PartitionInfo.GetLocation(i)
				fileType := scanNode.PartitionInfo.GetFileType(i)
				files := scanNode.PartitionInfo.GetPartitionFiles(i)
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
			for i, file := range scanNode.PartitionInfo.GetNoPartititonFiles() {
				parInfos[i%pn].FileList = append(parInfos[i%pn].FileList, file)
			}
		}

		var resScan []Job
		for i := 0; i < pn; i++ {
			resScan = append(resScan, NewScanJob(scanNode, parInfos[i], outputs[i], []*pb.Location{outputs[i]}))
		}

		*jobs = append(*jobs, resScan...)
		return resScan, nil

	case *plan.SelectNode:
		selectNode := node.(*plan.SelectNode)
		inputJobs, err := createJob(selectNode.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		if selectNode.SetQuantifier == nil || (*selectNode.SetQuantifier != gtype.DISTINCT) || len(inputJobs) == 1 {
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
					res = append(res, NewSelectJob(selectNode, input, output))
				}
			} else {
				nodeLoc := executorHeap.GetExecutorLoc()
				var outputs []*pb.Location
				for i := 0; i < pn; i++ {
					nodeLoc.ChannelIndex = int32(i)
					outputs = append(outputs, nodeLoc)

					selLoc := executorHeap.GetExecutorLoc()
					selLoc.ChannelIndex = 0
					res = append(res, NewSelectJob(selectNode, nodeLoc, selLoc))
				}
				*jobs = append(*jobs, NewBalanceJob(inputs, outputs))
			}

			*jobs = append(*jobs, res...)

		} else { //for select distinct
			aggLoc := executorHeap.GetExecutorLoc()
			aggLoc.ChannelIndex = 0
			var inputLocs []*pb.Location
			for _, inputNode := range inputJobs {
				inputLocs = append(inputLocs, inputNode.GetOutputs()...)
			}
			aggregateJob := NewAggregateJob(inputLocs, aggLoc)
			selectLoc := executorHeap.GetExecutorLoc()
			selectLoc.ChannelIndex = 0
			newSelectJob := NewSelectJob(selectNode, aggLoc, selectLoc)
			res = append(res, newSelectJob)
			*jobs = append(*jobs, aggregateJob, newSelectJob)
		}
		return res, nil

	case *plan.GroupByNode:
		groupByNode := node.(*plan.GroupByNode)
		inputJobs, err := createJob(groupByNode.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				output.ChannelIndex = 0
				res = append(res, NewGroupByJob(groupByNode, input, output))
			}
		}

		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.JoinNode:
		joinNode := node.(*plan.JoinNode)
		leftInputJobs, err1 := createJob(joinNode.LeftInput, jobs, executorHeap, pn)
		if err1 != nil {
			return nil, err1
		}
		rightInputJobs, err2 := createJob(joinNode.RightInput, jobs, executorHeap, pn)
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
			res = append(res, NewJoinJob(joinNode, leftInputs[i], rightInputs[i], output))
		}
		*jobs = append(*jobs, duplicateJob)
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.HashJoinNode:
		hashJoinNode := node.(*plan.HashJoinNode)
		leftInputJobs, err1 := createJob(hashJoinNode.LeftInput, jobs, executorHeap, pn)
		if err1 != nil {
			return nil, err1
		}
		rightInputJobs, err2 := createJob(hashJoinNode.RightInput, jobs, executorHeap, pn)
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
			for _, key := range hashJoinNode.LeftKeys {
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
			for _, key := range hashJoinNode.RightKeys {
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

		leftInputss, rightInputss := make([][]*pb.Location, pn), make([][]*pb.Location, pn)
		for _, node := range leftShuffleJobs {
			outputs := node.GetOutputs()
			for i, output := range outputs {
				leftInputss[i] = append(leftInputss[i], output)
			}
		}

		for _, node := range rightShuffleJobs {
			outputs := node.GetOutputs()
			for i, output := range outputs {
				rightInputss[i] = append(rightInputss[i], output)
			}
		}

		//hash join
		for i := 0; i < pn; i++ {
			output := executorHeap.GetExecutorLoc()
			output.ChannelIndex = 0
			res = append(res, NewEPlanHashJoinJob(hashJoinNode, leftInputss[i], rightInputss[i], output))
		}
		*jobs = append(*jobs, leftShuffleJobs...)
		*jobs = append(*jobs, rightShuffleJobs...)
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.LimitNode:
		limitNode := node.(*plan.LimitNode)
		inputJobs, err := createJob(limitNode.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}

		limitNodeLoc := executorHeap.GetExecutorLoc()
		limitNodeLoc.ChannelIndex = 0
		res = append(res, NewLimitJob(limitNode, inputs, limitNodeLoc))

		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.DistinctLocalNode:
		distinctLocalNode := node.(*plan.DistinctLocalNode)
		inputJobs, err := createJob(distinctLocalNode.Input, jobs, executorHeap, pn)
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
			res = append(res, NewDistinctLocalJob(distinctLocalNode, []*pb.Location{inputs[i]}, []*pb.Location{output}))
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.DistinctGlobalNode:
		distinctGlobalNode := node.(*plan.DistinctGlobalNode)
		inputJobs, err := createJob(distinctGlobalNode.Input, jobs, executorHeap, pn)
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
		distGlobalJob := NewDistinctGlobalJob(distinctGlobalNode, inputs, outputs)

		res = append(res, distGlobalJob)
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.AggregateNode:
		aggregateNode := node.(*plan.AggregateNode)
		inputJobs, err := createJob(aggregateNode.Input, jobs, executorHeap, pn)
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
		aggregateFuncGlobalNode := node.(*plan.AggregateFuncGlobalNode)
		inputJobs, err := createJob(aggregateFuncGlobalNode.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		output := executorHeap.GetExecutorLoc()
		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}
		res = append(res, NewAggregateFuncGlobalJob(aggregateFuncGlobalNode, inputs, output))
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.AggregateFuncLocalNode:
		planAggregateFuncLocalNode := node.(*plan.AggregateFuncLocalNode)
		inputJobs, err := createJob(planAggregateFuncLocalNode.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				output.ChannelIndex = 0
				res = append(res, NewAggregateFuncLocalJob(planAggregateFuncLocalNode, input, output))
			}
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.FilterNode:
		filterNode := node.(*plan.FilterNode)
		inputJobs, err := createJob(filterNode.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				output.ChannelIndex = 0
				res = append(res, NewFilterJob(filterNode, input, output))
			}
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.UnionNode:
		unionNode := node.(*plan.UnionNode)
		leftInputJobs, err1 := createJob(unionNode.LeftInput, jobs, executorHeap, pn)
		if err1 != nil {
			return nil, err1
		}
		rightInputJobs, err2 := createJob(unionNode.RightInput, jobs, executorHeap, pn)
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
			res = append(res, NewUnionJob(unionNode, leftInputs[i], rightInputs[i], output))
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *plan.OrderByNode:
		orderByNode := node.(*plan.OrderByNode)
		inputJobs, err := createJob(orderByNode.Input, jobs, executorHeap, pn)
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
			localRes = append(localRes, NewOrderByLocalJob(orderByNode, input, output))
		}

		inputs = []*pb.Location{}
		for _, inputJob := range localRes {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}
		output := executorHeap.GetExecutorLoc()
		newOrderByJob := NewOrderByJob(orderByNode, inputs, output)
		res = append(res, newOrderByJob)

		*jobs = append(*jobs, localRes...)
		*jobs = append(*jobs, newOrderByJob)
		return res, nil

	default:
		logger.Errorf("create job: unknown type")
		return nil, fmt.Errorf("create job: unknown type")

	}
}
