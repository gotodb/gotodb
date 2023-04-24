package executor

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
)

func (e *Executor) SetInstructionInserted(instruction *pb.Instruction) (err error) {
	var job stage.InsertedJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunInserted() error {
	md := &metadata.Metadata{}
	for _, reader := range e.Readers {
		if err := util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	//write rows
	var affectedRows int64 = 0
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

			for i := 0; i < rg.RowsNumber; i++ {
				affectedRows += rg.Vals[0][i].(int64)
			}
		}
	}

	writer := e.Writers[0]
	if err := util.WriteObject(writer, md); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(md, nil, writer)
	r := row.NewRow()
	r.AppendVals(affectedRows)
	if err := rbWriter.WriteRow(r); err != nil {
		return err
	}
	if err := rbWriter.Flush(); err != nil {
		return err
	}

	return nil
}
