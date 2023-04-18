package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/gtype"
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

func (e *Executor) RunUnion() error {
	writer := e.Writers[0]
	//read md
	if len(e.Readers) != 2 {
		return fmt.Errorf("union readers number %v <> 2", len(e.Readers))
	}

	readerMDs := make([]*metadata.Metadata, 2)
	for i, reader := range e.Readers {
		if err := util.ReadObject(reader, &readerMDs[i]); err != nil {
			return err
		}
	}

	if len(readerMDs[0].Columns) != len(readerMDs[1].Columns) {
		return fmt.Errorf("union metadata column number does not match")
	}

	md := readerMDs[0].Copy()

	for i, firstColumns := range readerMDs[0].Columns {
		secondColumns := readerMDs[1].Columns[i]
		if secondColumns.ColumnType >= firstColumns.ColumnType {
			md.Columns[i].ColumnType = secondColumns.ColumnType
		}
	}

	//write md
	if err := util.WriteObject(writer, md); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(md, nil, writer)
	//write rows
	var rg *row.RowsGroup
	var err error
	for index, reader := range e.Readers {
		rbReader := row.NewRowsBuffer(readerMDs[index], reader, nil)
		for {
			rg, err = rbReader.Read()
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return err
			}

			for i, column := range readerMDs[index].Columns {
				if column.ColumnType != md.Columns[i].ColumnType {
					for j := range rg.Vals {
						rg.Vals[i][j] = gtype.ToType(rg.Vals[i][j], md.Columns[i].ColumnType)
					}
				}
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
