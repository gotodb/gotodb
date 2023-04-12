package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
)

func (e *Executor) SetInstructionSelect(instruction *pb.Instruction) (err error) {
	var job stage.SelectJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunSelect() (err error) {
	job := e.StageJob.(*stage.SelectJob)
	md := &metadata.Metadata{}
	reader := e.Readers[0]
	writer := e.Writers[0]
	if err = util.ReadObject(reader, md); err != nil {
		return err
	}

	//write metadata
	if err = util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbReader, rbWriter := row.NewRowsBuffer(md, reader, nil), row.NewRowsBuffer(job.Metadata, nil, writer)
	defer func() {
		rbWriter.Flush()
	}()

	//init
	for _, item := range job.SelectItems {
		if err := item.Init(md); err != nil {
			return err
		}
	}
	distinctMap := make(map[string]bool)

	//write rows
	var rg, res *row.RowsGroup
	for {
		rg, err = rbReader.Read()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}

		if res, err = e.CalSelectItems(job, rg); err != nil {
			break
		}

		//for distinct
		if job.SetQuantifier != nil && (*job.SetQuantifier) == gtype.DISTINCT {
			for i := 0; i < res.GetRowsNumber(); i++ {
				r := res.GetRow(i)
				key := fmt.Sprintf("%v", r)
				if _, ok := distinctMap[key]; ok {
					continue
				}
				distinctMap[key] = true
				if err = rbWriter.WriteRow(r); err != nil {
					break
				}
			}

		} else {
			if err = rbWriter.Write(res); err != nil {
				break
			}
		}
	}

	return err
}

func (e *Executor) CalSelectItems(job *stage.SelectJob, rg *row.RowsGroup) (*row.RowsGroup, error) {
	var err error
	var vs []interface{}
	res := row.NewRowsGroup(job.Metadata)
	ci := 0

	if job.Having != nil {
		rgTmp := row.NewRowsGroup(rg.Metadata)
		flags, err := job.Having.Result(rg)
		if err != nil {
			return nil, err
		}

		for i, flag := range flags.([]interface{}) {
			if flag.(bool) {
				rgTmp.AppendRowVals(rg.GetRowVals(i)...)
				rgTmp.AppendRowKeys(rg.GetRowKeys(i)...)
			}
		}
		rg = rgTmp
	}

	for _, item := range job.SelectItems {
		vs, err = item.Result(rg)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if item.Expression == nil { //*
			for _, vi := range vs {
				res.Vals[ci] = append(res.Vals[ci], vi.([]interface{})...)
				ci++
			}

		} else {
			res.Vals[ci] = append(res.Vals[ci], vs[0].([]interface{})...)
			ci++
		}

		res.RowsNumber = len(vs)
	}

	return res, err
}
