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

func (e *Executor) SetInstructionOrderBy(instruction *pb.Instruction) (err error) {
	var job stage.OrderByJob
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
	e.OutputLocations = []*pb.Location{&job.Output}
	return nil
}

func (e *Executor) RunOrderBy() (err error) {
	f, _ := os.Create(fmt.Sprintf("executor_%v_orderby_%v_cpu.pprof", e.Name, time.Now().Format("20060102150405")))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		if err != nil {
			e.AddLogInfo(err, pb.LogLevel_ERR)
		}
		e.Clear()
	}()

	job := e.StageJob.(*stage.OrderByJob)
	md := &metadata.Metadata{}
	//read md
	for _, reader := range e.Readers {
		if err = util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	//write md
	writer := e.Writers[0]
	if err = util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbReaders := make([]*row.RowsBuffer, len(e.Readers))
	for i, reader := range e.Readers {
		rbReaders[i] = row.NewRowsBuffer(md, reader, nil)
	}
	rbWriter := row.NewRowsBuffer(job.Metadata, nil, writer)

	defer func() {
		rbWriter.Flush()
	}()

	//write rows
	var r *row.Row
	rs := row.NewRows(e.GetOrder(job))
	rs.Data = make([]*row.Row, len(e.Readers))

	isEnd := make([]bool, len(e.Readers))
	for {
		for i := 0; i < len(isEnd); i++ {
			if !isEnd[i] && rs.Data[i] == nil {
				r, err = rbReaders[i].ReadRow()
				if err == io.EOF {
					err = nil
					isEnd[i] = true
					continue
				}
				if err != nil {
					return err
				}
				rs.Data[i] = r
			}
		}

		if minIndex := rs.Min(); minIndex < 0 {
			break

		} else {
			rs.Data[minIndex].ClearKeys()
			if err = rbWriter.WriteRow(rs.Data[minIndex]); err != nil {
				return err
			}
			rs.Data[minIndex] = nil
		}
	}

	logger.Infof("RunOrderBy finished")
	return nil
}

func (e *Executor) GetOrder(job *stage.OrderByJob) []gtype.OrderType {
	var res []gtype.OrderType
	for _, item := range job.SortItems {
		res = append(res, item.OrderType)
	}
	return res
}