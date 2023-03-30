package executor

import (
	"github.com/gotodb/gotodb/plan/operator"
	"github.com/gotodb/gotodb/stage"
	"io"
	"sync"

	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionHashJoin(instruction *pb.Instruction) (err error) {
	var job stage.HashJoinJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	return nil
}

func CalHashKey(es []*operator.ExpressionNode, rg *row.RowsGroup) (string, error) {
	res := ""
	for _, e := range es {
		r, err := e.Result(rg)
		if err != nil {
			return res, err
		}
		res += gtype.ToKeyString(r.([]interface{})[0]) + ":"
	}
	return res, nil
}

func (e *Executor) RunHashJoin() (err error) {
	writer := e.Writers[0]
	job := e.StageJob.(*stage.HashJoinJob)

	//read md
	mds := make([]*metadata.Metadata, len(e.Readers))

	for i, reader := range e.Readers {
		mds[i] = &metadata.Metadata{}
		if err = util.ReadObject(reader, mds[i]); err != nil {
			return err
		}
	}
	leftNum := len(job.LeftInputs)
	leftReaders, rightReaders := e.Readers[:leftNum], e.Readers[leftNum:]
	leftMd, rightMd := mds[0], mds[leftNum]

	//write md
	if err = util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(job.Metadata, nil, writer)

	defer func() {
		rbWriter.Flush()
	}()

	//init
	if err := job.JoinCriteria.Init(job.Metadata); err != nil {
		return err
	}
	for _, k := range job.LeftKeys {
		if err := k.Init(leftMd); err != nil {
			return err
		}
	}
	for _, k := range job.RightKeys {
		if err := k.Init(rightMd); err != nil {
			return err
		}
	}

	//write rows
	rightRg := row.NewRowsGroup(rightMd)
	rowsMap := make(map[string][]int)

	switch job.JoinType {
	case plan.INNERJOIN:
		fallthrough
	case plan.LEFTJOIN:
		//read right
		var wg sync.WaitGroup
		var mutex sync.Mutex
		for i := range rightReaders {
			wg.Add(1)
			go func(index int) {
				defer func() {
					wg.Done()
				}()

				rightReader := rightReaders[index]
				rightRbReader := row.NewRowsBuffer(rightMd, rightReader, nil)
				for {
					rg, err := rightRbReader.Read()
					if err == io.EOF {
						err = nil
						break
					}
					if err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						return
					}
					mutex.Lock()
					rn := rightRg.GetRowsNumber()
					for i := 0; i < rg.GetRowsNumber(); i++ {
						key := rg.GetKeyString(i)
						if _, ok := rowsMap[key]; ok {
							rowsMap[key] = append(rowsMap[key], rn+i)
						} else {
							rowsMap[key] = []int{rn + i}
						}
					}
					rightRg.AppendRowGroupRows(rg)
					mutex.Unlock()
				}
			}(i)
		}
		wg.Wait()

		//read left
		for i := range leftReaders {
			wg.Add(1)
			go func(index int) {
				defer func() {
					wg.Done()
				}()
				leftReader := leftReaders[index]
				leftRbReader := row.NewRowsBuffer(leftMd, leftReader, nil)
				for {
					rg, err := leftRbReader.Read()
					if err == io.EOF {
						err = nil
						break
					}
					if err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						return
					}

					for i := 0; i < rg.GetRowsNumber(); i++ {
						r := rg.GetRow(i)
						leftKey := r.GetKeyString()
						joinNum := 0
						if _, ok := rowsMap[leftKey]; ok {
							for _, i := range rowsMap[leftKey] {
								rightRow := rightRg.GetRow(i)
								joinRow := row.RowPool.Get().(*row.Row)
								joinRow.Clear()
								joinRow.AppendVals(r.Vals...)
								joinRow.AppendVals(rightRow.Vals...)
								rg := row.NewRowsGroup(job.Metadata)
								rg.Write(joinRow)
								if ok, err := job.JoinCriteria.Result(rg); ok && err == nil {
									if err = rbWriter.WriteRow(joinRow); err != nil {
										e.AddLogInfo(err, pb.LogLevel_ERR)
										return
									}
									joinNum++
								} else if err != nil {
									e.AddLogInfo(err, pb.LogLevel_ERR)
									return
								}
								row.RowPool.Put(rightRow)
								row.RowPool.Put(joinRow)
							}
						}

						if job.JoinType == plan.LEFTJOIN && joinNum == 0 {
							joinRow := row.NewRow(r.Vals...)
							joinRow.AppendVals(make([]interface{}, len(mds[1].GetColumnNames()))...)
							if err = rbWriter.WriteRow(joinRow); err != nil {
								e.AddLogInfo(err, pb.LogLevel_ERR)
								return
							}
						}

						row.RowPool.Put(r)
					}
				}
			}(i)
		}

		wg.Wait()

	case plan.RIGHTJOIN:

	}

	logger.Infof("RunJoin finished")
	return err
}
