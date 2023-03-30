package executor

import (
	"github.com/gotodb/gotodb/stage"
	"io"
	"sync"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/logger"
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
	e.Instruction = instruction
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

	//write rows
	jobs := make(chan *row.Row)
	var wg sync.WaitGroup

	//init
	for _, be := range job.BooleanExpressions {
		if err := be.Init(md); err != nil {
			return err
		}
	}

	for i := 0; i < int(config.Conf.Runtime.ParallelNumber); i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()

			for {
				r, ok := <-jobs
				//log.Println("========Filiter", row, ok)
				if ok {
					rg := row.NewRowsGroup(md)
					rg.Write(r)
					flag := true
					for _, booleanExpression := range job.BooleanExpressions {
						if ok, err := booleanExpression.Result(rg); err == nil && !ok.([]interface{})[0].(bool) {
							flag = false
							break
						} else if err != nil {
							flag = false
							break
						}
					}

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

	logger.Infof("RunFilter finished")
	return err
}
