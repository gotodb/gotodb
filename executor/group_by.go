package executor

import (
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
)

func (e *Executor) SetInstructionGroupBy(instruction *pb.Instruction) (err error) {
	var job stage.GroupByJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	return nil
}

func (e *Executor) RunGroupBy() (err error) {
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
