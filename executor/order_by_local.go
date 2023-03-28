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

func (e *Executor) SetInstructionOrderByLocal(instruction *pb.Instruction) (err error) {
	var job stage.OrderByLocalJob
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	e.InputLocations = []*pb.Location{&job.Input}
	e.OutputLocations = []*pb.Location{&job.Output}
	return nil
}

func (e *Executor) RunOrderByLocal() (err error) {
	f, _ := os.Create(fmt.Sprintf("executor_%v_orderbylocal_%v_cpu.pprof", e.Name, time.Now().Format("20060102150405")))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		e.AddLogInfo(err, pb.LogLevel_ERR)
		e.Clear()
	}()

	reader, writer := e.Readers[0], e.Writers[0]
	job := e.StageJob.(*stage.OrderByLocalJob)
	md := &metadata.Metadata{}

	//read md
	if err = util.ReadObject(reader, md); err != nil {
		return err
	}

	//write md
	job.Metadata.ClearKeys()
	for _, item := range job.SortItems {
		t, err := item.GetType(md)
		if err != nil {
			return err
		}
		job.Metadata.AppendKeyByType(t)
	}
	if err = util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(job.Metadata, nil, writer)
	rbReader := row.NewRowsBuffer(md, reader, nil)

	defer func() {
		rbWriter.Flush()
	}()

	//init
	for _, item := range job.SortItems {
		if err := item.Init(md); err != nil {
			return err
		}
	}

	//write rows
	var r *row.Row
	rs := row.NewRows(e.GetOrderLocal(job))

	for {
		r, err = rbReader.ReadRow()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return err
		}
		rg := row.NewRowsGroup(md)
		rg.Write(r)
		r.Keys, err = e.CalSortKey(job, rg)
		if err != nil {
			return err
		}
		rs.Append(r)
	}
	rs.Sort()
	for _, r := range rs.Data {
		if err = rbWriter.WriteRow(r); err != nil {
			return err
		}
	}

	logger.Infof("RunOrderByLocal finished")
	return nil
}

func (e *Executor) GetOrderLocal(job *stage.OrderByLocalJob) []gtype.OrderType {
	var res []gtype.OrderType
	for _, item := range job.SortItems {
		res = append(res, item.OrderType)
	}
	return res
}

func (e *Executor) CalSortKey(job *stage.OrderByLocalJob, rg *row.RowsGroup) ([]interface{}, error) {
	var err error
	var res []interface{}
	for _, item := range job.SortItems {
		key, err := item.Result(rg)
		if err == io.EOF {
			return res, nil
		}
		if err != nil {
			return res, err
		}
		res = append(res, key)
	}

	return res, err

}
