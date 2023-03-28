package executor

import (
	"github.com/gotodb/gotodb/stage"
	"io"

	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionAggregate(instruction *pb.Instruction) (err error) {
	var job stage.AggregateJob
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

func (e *Executor) RunAggregate() (err error) {
	writer := e.Writers[0]

	md := &metadata.Metadata{}
	//read md
	for _, reader := range e.Readers {
		if err = util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	//write md
	if err = util.WriteObject(writer, md); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(md, nil, writer)

	//write rows
	var rg *row.RowsGroup
	for _, reader := range e.Readers {
		rbReader := row.NewRowsBuffer(md, reader, nil)
		for {
			rg, err = rbReader.Read()
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
	rbWriter.Flush()
	logger.Infof("RunAggregate finished")
	return nil
}
