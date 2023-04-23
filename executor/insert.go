package executor

import (
	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionInsert(instruction *pb.Instruction) (err error) {
	var job stage.InsertJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunInsert() error {
	job := e.StageJob.(*stage.InsertJob)
	ctr, err := connector.NewConnector(job.Catalog, job.Schema, job.Table)
	if err != nil {
		return err
	}

	md := &metadata.Metadata{}
	reader := e.Readers[0]
	writer := e.Writers[0]
	if err := util.ReadObject(reader, md); err != nil {
		return err
	}

	//write metadata
	if err := util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbReader := row.NewRowsBuffer(md, reader, nil)
	affectedRows, err := ctr.Insert(rbReader, job.Columns)
	if err != nil {
		return err
	}

	r := row.NewRow()
	r.AppendVals(affectedRows)
	rbWriter := row.NewRowsBuffer(job.Metadata, nil, writer)
	if err = rbWriter.WriteRow(r); err != nil {
		return err
	}

	if err := rbWriter.Flush(); err != nil {
		return err
	}

	return nil
}
