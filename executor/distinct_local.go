package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/stage"
	"io"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionDistinctLocal(instruction *pb.Instruction) (err error) {
	var job stage.DistinctLocalJob
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	e.InputLocations = []*pb.Location{}
	for i := 0; i < len(job.Inputs); i++ {
		loc := job.Inputs[i]
		e.InputLocations = append(e.InputLocations, &loc)
	}
	e.OutputLocations = []*pb.Location{}
	for i := 0; i < len(job.Outputs); i++ {
		loc := job.Outputs[i]
		e.OutputLocations = append(e.OutputLocations, &loc)
	}
	return nil
}

func (e *Executor) RunDistinctLocal() (err error) {
	f, _ := os.Create(fmt.Sprintf("executor_%v_distinctlocal_%v_cpu.pprof", e.Name, time.Now().Format("20060102150405")))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		if err != nil {
			e.AddLogInfo(err, pb.LogLevel_ERR)
		}
		e.Clear()
	}()
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
	for _, writer := range e.Writers {
		if err = util.WriteObject(writer, mdOutput); err != nil {
			return err
		}
	}

	rbWriters := make([]*row.RowsBuffer, len(e.Writers))
	for i, writer := range e.Writers {
		rbWriters[i] = row.NewRowsBuffer(mdOutput, nil, writer)
	}

	defer func() {
		for _, rbWriter := range rbWriters {
			rbWriter.Flush()
		}
	}()

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
	for i, _ := range e.Readers {
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
						ckey := gtype.ToKeyString(c)
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

					row.RowPool.Put(r)
				}
			}
		}(i)
	}

	wg.Wait()

	return nil
}
