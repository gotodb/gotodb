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

func (e *Executor) SetInstructionBalance(instruction *pb.Instruction) (err error) {
	var job stage.BalanceJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunBalance() error {
	//read md
	md := &metadata.Metadata{}
	for _, reader := range e.Readers {
		if err := util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	mdOutput := md.Copy()

	rbWriters := make([]*row.RowsBuffer, len(e.Writers))
	for i, writer := range e.Writers {
		if err := util.WriteObject(writer, mdOutput); err != nil {
			return err
		}
		rbWriters[i] = row.NewRowsBuffer(mdOutput, nil, writer)
	}

	//write rows
	i, wn := 0, len(rbWriters)
	for _, reader := range e.Readers {
		rbReader := row.NewRowsBuffer(md, reader, nil)
		for {
			rg, err := rbReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			rbWriter := rbWriters[i]
			i++
			i = i % wn

			if err = rbWriter.Write(rg); err != nil {
				return err
			}
		}
	}

	for _, rbWriter := range rbWriters {
		if err := rbWriter.Flush(); err != nil {
			return err
		}
	}

	return nil
}
