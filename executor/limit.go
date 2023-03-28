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

func (e *Executor) SetInstructionLimit(instruction *pb.Instruction) (err error) {
	var job stage.LimitJob
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	e.InputLocations = []*pb.Location{}
	for i := 0; i < len(job.Inputs); i++ {
		e.InputLocations = append(e.InputLocations, &(job.Inputs[i]))
	}
	e.OutputLocations = []*pb.Location{&job.Output}
	return nil
}

func (e *Executor) RunLimit() (err error) {
	job := e.StageJob.(*stage.LimitJob)
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

	rbReaders := make([]*row.RowsBuffer, len(e.Readers))
	for i, reader := range e.Readers {
		rbReaders[i] = row.NewRowsBuffer(md, reader, nil)
	}
	rbWriter := row.NewRowsBuffer(md, nil, writer)

	defer func() {
		rbWriter.Flush()
	}()

	//write rows
	var rg *row.RowsGroup
	readRowCnt := int64(0)
	for _, rbReader := range rbReaders {
		for readRowCnt < *(job.LimitNumber) {
			rg, err = rbReader.Read()
			if err == io.EOF || readRowCnt >= *(job.LimitNumber) {
				err = nil
				break
			}
			if err != nil {
				return err
			}
			if readRowCnt+int64(rg.GetRowsNumber()) <= *(job.LimitNumber) {
				readRowCnt += int64(rg.GetRowsNumber())
				if err = rbWriter.Write(rg); err != nil {
					return err
				}

			} else {
				for readRowCnt < *(job.LimitNumber) {
					r, err := rg.Read()
					if err != nil {
						return err
					}
					if err = rbWriter.WriteRow(r); err != nil {
						return err
					}
					readRowCnt++

				}
			}
		}
	}

	logger.Infof("RunAggregate finished")
	return nil
}
