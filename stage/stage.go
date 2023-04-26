package stage

import (
	"fmt"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/partition"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/planner/operator"
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
	JobTypeInsert
	JobTypeInserted
)

func (s JobType) String() string {
	switch s {
	case JobTypeScan:
		return "SCAN"
	case JobTypeInsert:
		return "INSERT"
	case JobTypeInserted:
		return "INSERTED"
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

func CreateJob(node planner.Plan, executorHeap Worker, pn int) ([]Job, error) {
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

func createJob(inode planner.Plan, jobs *[]Job, executorHeap Worker, pn int) ([]Job, error) {
	var res []Job
	switch node := inode.(type) {
	case *planner.ShowPlan:
		output := executorHeap.GetExecutorLoc()
		res = append(res, NewShowJob(node, output))
		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.ScanPlan:
		scanNodePar, err := node.Connector.GetPartition(pn)
		if err != nil {
			return res, err
		}

		partitions := make([]*partition.Partition, pn)
		recMap := make([]map[int]int, pn)
		for i := 0; i < pn; i++ {
			partitions[i] = partition.New(scanNodePar.Metadata)
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
						recMap[k][i] = partitions[k].GetPartitionNum()
						partitions[k].Write(row)
						partitions[k].Locations = append(partitions[k].Locations, location)
						partitions[k].FileTypes = append(partitions[k].FileTypes, fileType)
						partitions[k].FileLists = append(partitions[k].FileLists, []*partition.FileLocation{})
					}
					j := recMap[k][i]
					partitions[k].FileLists[j] = append(partitions[k].FileLists[j], file)

					k++
					k = k % pn
				}
			}
		} else {
			for i, file := range scanNodePar.GetNoPartitionFiles() {
				partitions[i%pn].Locations = append(partitions[i%pn].Locations, file.Location)
				partitions[i%pn].FileTypes = append(partitions[i%pn].FileTypes, file.FileType)
			}
		}

		var resScan []Job
		for i := 0; i < pn; i++ {
			if len(partitions[i].Locations) > 0 {
				output := executorHeap.GetExecutorLoc()
				resScan = append(resScan, NewScanJob(node, partitions[i], output, []*pb.Location{output}))
			}
		}

		*jobs = append(*jobs, resScan...)
		return resScan, nil

	case *planner.SelectPlan:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		if node.SetQuantifier == nil || (*node.SetQuantifier != datatype.DISTINCT) || len(inputJobs) == 1 {
			var inputs []*pb.Location
			for _, inputNode := range inputJobs {
				inputs = append(inputs, inputNode.GetOutputs()...)
			}
			ln := len(inputs)
			if ln > 1 {
				for i := 0; i < ln; i++ {
					output := executorHeap.GetExecutorLoc()
					res = append(res, NewSelectJob(node, inputs[i], output))
				}
			} else {
				nodeLoc := executorHeap.GetExecutorLoc()
				var outputs []*pb.Location
				for i := 0; i < pn; i++ {
					nodeChannelLoc := nodeLoc.NewChannel(int32(i))
					outputs = append(outputs, nodeChannelLoc)

					selLoc := executorHeap.GetExecutorLoc()
					res = append(res, NewSelectJob(node, nodeChannelLoc, selLoc))
				}
				*jobs = append(*jobs, NewBalanceJob(inputs, outputs))
			}

			*jobs = append(*jobs, res...)

		} else {
			//for select distinct
			aggLoc := executorHeap.GetExecutorLoc()
			var inputLocs []*pb.Location
			for _, inputNode := range inputJobs {
				inputLocs = append(inputLocs, inputNode.GetOutputs()...)
			}
			aggregateJob := NewAggregateJob(inputLocs, aggLoc)
			selectLoc := executorHeap.GetExecutorLoc()
			newSelectJob := NewSelectJob(node, aggLoc, selectLoc)
			res = append(res, newSelectJob)
			*jobs = append(*jobs, aggregateJob, newSelectJob)
		}
		return res, nil

	case *planner.InsertPlan:
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
			localRes = append(localRes, NewInsertJob(node, input, output))
		}

		inputs = []*pb.Location{}
		for _, inputJob := range localRes {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}
		output := executorHeap.GetExecutorLoc()
		newInsertedJob := NewInsertedJob(node, inputs, output)
		res = append(res, newInsertedJob)

		*jobs = append(*jobs, localRes...)
		*jobs = append(*jobs, newInsertedJob)
		return res, nil
	case *planner.GroupByPlan:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				res = append(res, NewGroupByJob(node, input, output))
			}
		}

		*jobs = append(*jobs, res...)
		return res, nil
	case *planner.JoinPlan:
		leftInputJobs, err1 := createJob(node.LeftInput, jobs, executorHeap, pn)
		if err1 != nil {
			return nil, err1
		}
		rightInputJobs, err2 := createJob(node.RightInput, jobs, executorHeap, pn)
		if err2 != nil {
			return nil, err2
		}

		//duplicate right inputs
		var rightInputs []*pb.Location
		for _, rightInputJob := range rightInputJobs {
			rightInputs = append(rightInputs, rightInputJob.GetOutputs()...)
		}

		var leftInputs []*pb.Location
		for _, leftInputJob := range leftInputJobs {
			leftInputs = append(leftInputs, leftInputJob.GetOutputs()...)
		}

		if len(leftInputs) > len(rightInputs) {

			var outputs []*pb.Location
			loc := executorHeap.GetExecutorLoc()
			for i := 0; i < len(leftInputs); i++ {
				outputs = append(outputs, loc.NewChannel(int32(i)))
			}
			duplicateJob := NewDuplicateJob(rightInputs, outputs, nil)
			*jobs = append(*jobs, duplicateJob)
			rightInputs = duplicateJob.GetOutputs()

		} else if len(rightInputs) > len(leftInputs) {

			var outputs []*pb.Location
			loc := executorHeap.GetExecutorLoc()
			for i := 0; i < len(rightInputs); i++ {
				outputs = append(outputs, loc.NewChannel(int32(i)))
			}
			duplicateJob := NewDuplicateJob(leftInputs, outputs, nil)
			*jobs = append(*jobs, duplicateJob)
			leftInputs = duplicateJob.GetOutputs()

		}

		for i := 0; i < len(leftInputs); i++ {
			output := executorHeap.GetExecutorLoc()
			res = append(res, NewJoinJob(node, leftInputs[i], rightInputs[i], output))
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.HashJoinPlan:
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
			loc := executorHeap.GetExecutorLoc()
			for i := 0; i < pn; i++ {
				outputs = append(outputs, loc.NewChannel(int32(i)))
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
			loc := executorHeap.GetExecutorLoc()
			for i := 0; i < pn; i++ {
				outputs = append(outputs, loc.NewChannel(int32(i)))
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
			res = append(res, NewHashJoinJob(node, leftJoinInputs[i], rightJoinInputs[i], output))
		}
		*jobs = append(*jobs, leftShuffleJobs...)
		*jobs = append(*jobs, rightShuffleJobs...)
		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.LimitPlan:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		var inputs []*pb.Location
		for _, inputJob := range inputJobs {
			inputs = append(inputs, inputJob.GetOutputs()...)
		}

		limitNodeLoc := executorHeap.GetExecutorLoc()
		res = append(res, NewLimitJob(node, inputs, limitNodeLoc))

		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.DistinctLocalPlan:
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
			res = append(res, NewDistinctLocalJob(node, []*pb.Location{inputs[i]}, []*pb.Location{output}))
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.DistinctGlobalPlan:
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
			outputs = append(outputs, loc.NewChannel(int32(i)))
		}
		distGlobalJob := NewDistinctGlobalJob(node, inputs, outputs)

		res = append(res, distGlobalJob)
		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.AggregatePlan:
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

	case *planner.AggregateFuncGlobalPlan:
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

	case *planner.AggregateFuncLocalPlan:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				res = append(res, NewAggregateFuncLocalJob(node, input, output))
			}
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.FilterPlan:
		inputJobs, err := createJob(node.Input, jobs, executorHeap, pn)
		if err != nil {
			return res, err
		}
		for _, inputJob := range inputJobs {
			for _, input := range inputJob.GetOutputs() {
				output := executorHeap.GetExecutorLoc()
				res = append(res, NewFilterJob(node, input, output))
			}
		}
		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.UnionPlan:
		leftInputJobs, err := createJob(node.LeftInput, jobs, executorHeap, pn)
		if err != nil {
			return nil, err
		}
		rightInputJobs, err := createJob(node.RightInput, jobs, executorHeap, pn)
		if err != nil {
			return nil, err
		}

		var leftInputs []*pb.Location
		for _, leftInputJob := range leftInputJobs {
			leftInputs = append(leftInputs, leftInputJob.GetOutputs()...)
		}
		var rightInputs []*pb.Location
		for _, rightInputJob := range rightInputJobs {
			rightInputs = append(rightInputs, rightInputJob.GetOutputs()...)
		}

		if len(leftInputs) > len(rightInputs) {

			var outputs []*pb.Location
			output := executorHeap.GetExecutorLoc()
			for i := 0; i < len(leftInputs); i++ {
				outputs = append(outputs, output.NewChannel(int32(i)))
			}
			balanceJob := NewBalanceJob(rightInputs, outputs)
			*jobs = append(*jobs, balanceJob)
			rightInputs = balanceJob.GetOutputs()

		} else if len(rightInputs) > len(leftInputs) {

			var outputs []*pb.Location
			output := executorHeap.GetExecutorLoc()
			for i := 0; i < len(rightInputs); i++ {
				outputs = append(outputs, output.NewChannel(int32(i)))
			}
			balanceJob := NewBalanceJob(leftInputs, outputs)
			*jobs = append(*jobs, balanceJob)
			leftInputs = balanceJob.GetOutputs()

		}

		for i := 0; i < len(leftInputs); i++ {
			output := executorHeap.GetExecutorLoc()
			res = append(res, NewUnionJob(node, leftInputs[i], rightInputs[i], output))
		}

		*jobs = append(*jobs, res...)
		return res, nil

	case *planner.OrderByPlan:
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
		return nil, fmt.Errorf("create job: unknown type")
	}
}
