package executor

import (
	"github.com/gotodb/gotodb/stage"
	"io"

	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionAggregate(instruction *pb.Instruction) error {
	var job stage.AggregateJob
	if err := msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunAggregate() error {

	md := &metadata.Metadata{}
	//read md
	for _, reader := range e.Readers {
		if err := util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	//write md
	writer := e.Writers[0]
	if err := util.WriteObject(writer, md); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(md, nil, writer)

	//write rows
	for _, reader := range e.Readers {
		rbReader := row.NewRowsBuffer(md, reader, nil)
		for {
			rg, err := rbReader.Read()
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return err
			}
			if err = rbWriter.Write(rg); err != nil {
				return err
			}
		}
	}

	if err := rbWriter.Flush(); err != nil {
		return err
	}
	return nil
}
