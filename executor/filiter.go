package executor

import (
	"github.com/gotodb/gotodb/stage"
	"io"
	"sync"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionFilter(instruction *pb.Instruction) (err error) {
	var job stage.FilterJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunFilter() (err error) {
	job := e.StageJob.(*stage.FilterJob)

	md := &metadata.Metadata{}
	reader := e.Readers[0]
	writer := e.Writers[0]
	if err = util.ReadObject(reader, md); err != nil {
		return err
	}

	//write metadata
	if err = util.WriteObject(writer, md); err != nil {
		return err
	}

	rbReader := row.NewRowsBuffer(md, reader, nil)
	rbWriter := row.NewRowsBuffer(md, nil, writer)

	var subQueryRows [][]*row.Row
	readerIndex := 1
	for _, be := range job.BooleanExpressions {
		if err := be.Init(md); err != nil {
			return err
		}

		if be.IsSetSubQuery() {
			subQueryMD := &metadata.Metadata{}
			if err := util.ReadObject(e.Readers[readerIndex], subQueryMD); err != nil {
				return err
			}
			subReader := row.NewRowsBuffer(subQueryMD, e.Readers[readerIndex], nil)
			var rs []*row.Row
			for {
				r, err := subReader.ReadRow()
				if err == io.EOF {
					err = nil
					break
				}
				if err != nil {
					return err
				}
				rs = append(rs, r)
			}
			subQueryRows = append(subQueryRows, rs)
			readerIndex++
		}
	}

	//write rows
	jobs := make(chan *row.Row)
	var wg sync.WaitGroup
	for i := 0; i < config.Conf.Runtime.ParallelNumber; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()

			for {
				r, ok := <-jobs
				if ok {
					rg := row.NewRowsGroup(md)

					rg.Write(r)
					flag := true
					readerIndex = 0
					for _, booleanExpression := range job.BooleanExpressions {
						if booleanExpression.IsSetSubQuery() {
							for _, subQueryRow := range subQueryRows[readerIndex] {
								// rows to columns
								rg.AppendKeyColumns(subQueryRow.Vals)
							}
						}
						readerIndex++
						if ok, err := booleanExpression.Result(rg); err == nil && !ok.([]interface{})[0].(bool) {
							flag = false
							break
						} else if err != nil {
							flag = false
							break
						}
					}
					readerIndex = 0
					if flag {
						err = rbWriter.WriteRow(r)
					}

					if err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						break
					}

				} else {
					break
				}
			}
		}()
	}

	var r *row.Row
	for err == nil {
		r, err = rbReader.ReadRow()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}
		jobs <- r
	}
	close(jobs)
	wg.Wait()

	if err = rbWriter.Flush(); err != nil {
		return err
	}

	return err
}
