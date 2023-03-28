package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/stage"
	"io"
	"os"
	"runtime/pprof"
	"time"

	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionGroupBy(instruction *pb.Instruction) (err error) {
	var job stage.GroupByJob
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	e.InputLocations = []*pb.Location{&job.Input}
	e.OutputLocations = []*pb.Location{&job.Output}
	return nil
}

func (e *Executor) RunGroupBy() (err error) {
	logger.Infof("RunGroupBy")
	f, _ := os.Create(fmt.Sprintf("executor_%v_groupby_%v_cpu.pprof", e.Name, time.Now().Format("20060102150405")))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		e.AddLogInfo(err, pb.LogLevel_ERR)
		e.Clear()
	}()

	job := e.StageJob.(*stage.GroupByJob)

	md := &metadata.Metadata{}
	reader := e.Readers[0]
	writer := e.Writers[0]
	if err = util.ReadObject(reader, md); err != nil {
		return err
	}

	//write metadata
	job.Metadata.ClearKeys()
	job.Metadata.AppendKeyByType(gtype.STRING)
	if err = util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbReader := row.NewRowsBuffer(md, reader, nil)
	rbWriter := row.NewRowsBuffer(job.Metadata, nil, writer)

	defer func() {
		rbWriter.Flush()
	}()

	//group by

	if err := job.GroupBy.Init(md); err != nil {
		return err
	}
	var rg *row.RowsGroup
	for {
		rg, err = rbReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		keys, err := job.GroupBy.Result(rg)
		if err != nil {
			return err
		}
		rg.AppendKeyColumns(keys)

		if err := rbWriter.Write(rg); err != nil {
			return err
		}
	}

	return err
}
