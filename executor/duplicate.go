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

func (e *Executor) SetInstructionDuplicate(instruction *pb.Instruction) (err error) {
	var job stage.DuplicateJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job

	return nil
}

func (e *Executor) RunDuplicate() (err error) {
	job := e.StageJob.(*stage.DuplicateJob)
	//read md
	md := &metadata.Metadata{}
	for _, reader := range e.Readers {
		if err = util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	mdOutput := md.Copy()

	//write md
	if job.Keys != nil && len(job.Keys) > 0 {
		mdOutput.ClearKeys()
		mdOutput.AppendKeyByType(gtype.STRING)
	}
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
	for _, k := range job.Keys {
		if err := k.Init(md); err != nil {
			return err
		}
	}

	//write rows
	var rg *row.RowsGroup
	for _, reader := range e.Readers {
		rbReader := row.NewRowsBuffer(md, reader, nil)
		for {
			rg, err = rbReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			for _, rbWriter := range rbWriters {
				if err = rbWriter.Write(rg); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
