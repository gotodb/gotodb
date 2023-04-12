package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
)

func (e *Executor) SetInstructionUnion(instruction *pb.Instruction) (err error) {
	var job stage.UnionJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunUnion() (err error) {
	writer := e.Writers[0]
	//read md
	if len(e.Readers) != 2 {
		return fmt.Errorf("union readers number %v <> 2", len(e.Readers))
	}

	md := &metadata.Metadata{}
	if len(e.Readers) != 2 {
		return fmt.Errorf("union input number error")
	}
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
	defer func() {
		rbWriter.Flush()
	}()

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

	return err
}
