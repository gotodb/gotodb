package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/stage"
	"io"
	"os"
	"runtime/pprof"
	"time"

	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionAggregateFuncGlobal(instruction *pb.Instruction) (err error) {
	var job stage.AggregateFuncGlobalJob
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	e.InputLocations = []*pb.Location{}
	for i := 0; i < len(job.Inputs); i++ {
		e.InputLocations = append(e.InputLocations, &job.Inputs[i])
	}
	e.OutputLocations = []*pb.Location{&job.Output}
	return nil
}

func (e *Executor) RunAggregateFuncGlobal() (err error) {
	f, _ := os.Create(fmt.Sprintf("executor_%v_aggregatefunclocal_%v_cpu.pprof", e.Name, time.Now().Format("20060102150405")))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		if err != nil {
			e.AddLogInfo(err, pb.LogLevel_ERR)
		}
		e.Clear()
	}()

	writer := e.Writers[0]
	job := e.StageJob.(*stage.AggregateFuncGlobalJob)
	md := &metadata.Metadata{}

	//read md
	for _, reader := range e.Readers {
		if err = util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	//write md
	if err = util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(job.Metadata, nil, writer)

	defer func() {
		rbWriter.Flush()
	}()

	//init
	if err := job.Init(job.Metadata); err != nil {
		return err
	}

	//write rows
	var rg *row.RowsGroup
	res := make([]map[string]interface{}, len(job.FuncNodes))
	for i := 0; i < len(res); i++ {
		res[i] = make(map[string]interface{})
	}

	keys := map[string]*row.Row{}

	for _, reader := range e.Readers { //TODO: concurrent?
		rbReader := row.NewRowsBuffer(md, reader, nil)
		for {
			rg, err = rbReader.Read()

			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				break
			}

			for i := 0; i < rg.GetRowsNumber(); i++ {
				key := rg.GetKeyString(i)
				if _, ok := keys[key]; !ok {
					keys[key] = rg.GetRow(i)
				}
			}

			if err = e.CalAggregateFuncGlobal(job, rg, &res); err != nil {
				break
			}
		}
	}

	for key, r := range keys {
		for i := 0; i < len(res); i++ {
			r.Vals[len(r.Vals)-i-1] = res[i][key]
		}
		rbWriter.WriteRow(r)
	}

	logger.Infof("RunAggregateFuncGlobal finished")
	return err
}

func (e *Executor) CalAggregateFuncGlobal(job *stage.AggregateFuncGlobalJob, rg *row.RowsGroup, res *[]map[string]interface{}) error {
	var err error
	var resc map[string]interface{}
	var resci interface{}
	for i, item := range job.FuncNodes {
		if resci, err = item.Result(rg); err != nil {
			break
		}
		resc = resci.(map[string]interface{})
		for k, v := range resc {
			(*res)[i][k] = v
		}
	}
	return err
}