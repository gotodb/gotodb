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

func (e *Executor) SetInstructionLimit(instruction *pb.Instruction) error {
	var job stage.LimitJob
	if err := msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunLimit() error {
	job := e.StageJob.(*stage.LimitJob)
	writer := e.Writers[0]
	md := &metadata.Metadata{}
	//read md
	rbReaders := make([]*row.RowsBuffer, len(e.Readers))
	for i, reader := range e.Readers {
		if err := util.ReadObject(reader, md); err != nil {
			return err
		}
		rbReaders[i] = row.NewRowsBuffer(md, reader, nil)
	}

	//write md
	if err := util.WriteObject(writer, md); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(md, nil, writer)
	readRowCnt := int64(0)
	for _, rbReader := range rbReaders {
		for readRowCnt < *(job.LimitNumber) {
			rg, err := rbReader.Read()
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

	if err := rbWriter.Flush(); err != nil {
		return err
	}

	return nil
}
