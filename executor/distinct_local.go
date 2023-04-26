package executor

import (
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
	"sync"
)

func (e *Executor) SetInstructionDistinctLocal(instruction *pb.Instruction) (err error) {
	var job stage.DistinctLocalJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job

	return nil
}

func (e *Executor) RunDistinctLocal() (err error) {
	job := e.StageJob.(*stage.DistinctLocalJob)
	//read md
	md := &metadata.Metadata{}
	for _, reader := range e.Readers {
		if err = util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	mdOutput := job.Metadata

	//write md
	rbWriters := make([]*row.RowsBuffer, len(e.Writers))
	for i, writer := range e.Writers {
		if err = util.WriteObject(writer, mdOutput); err != nil {
			return err
		}
		rbWriters[i] = row.NewRowsBuffer(mdOutput, nil, writer)
	}

	//init
	for _, e := range job.Expressions {
		if err := e.Init(md); err != nil {
			return err
		}
	}

	var mutex sync.Mutex
	distinctMap := make([]map[string]bool, len(job.Expressions))
	for i := 0; i < len(job.Expressions); i++ {
		distinctMap[i] = make(map[string]bool)
	}

	//write rows
	var wg sync.WaitGroup
	for i := range e.Readers {
		wg.Add(1)
		go func(index int) {
			defer func() {
				wg.Done()
			}()
			reader := e.Readers[index]
			rbReader := row.NewRowsBuffer(md, reader, nil)
			for {
				rg0, err := rbReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					e.AddLogInfo(err, pb.LogLevel_ERR)
					return
				}

				distCols := make([][]interface{}, len(job.Expressions))
				for i, exp := range job.Expressions {
					res, err := exp.Result(rg0)
					if err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						return
					}
					distCols[i] = res.([]interface{})
					mutex.Lock()
					for j, c := range distCols[i] {
						ckey := datatype.ToKeyString(c)
						if _, ok := distinctMap[i][ckey]; ok {
							distCols[i][j] = nil
						} else {
							distinctMap[i][ckey] = true
						}
					}
					mutex.Unlock()
				}

				for i := 0; i < rg0.GetRowsNumber(); i++ {
					r := rg0.GetRow(i)
					for _, c := range distCols {
						r.AppendVals(c[i])
					}
					if err = rbWriters[index].WriteRow(r); err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						return
					}

					row.Pool.Put(r)
				}
			}
		}(i)
	}

	wg.Wait()

	for _, rbWriter := range rbWriters {
		if err := rbWriter.Flush(); err != nil {
			return err
		}
	}

	return nil
}
