package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/planner"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
)

func (e *Executor) SetInstructionJoin(instruction *pb.Instruction) error {
	var job stage.JoinJob
	if err := msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunJoin() error {
	writer := e.Writers[0]
	job := e.StageJob.(*stage.JoinJob)

	//read md
	if len(e.Readers) != 2 {
		return fmt.Errorf("join readers number %v <> 2", len(e.Readers))
	}

	mds := make([]*metadata.Metadata, 2)
	for i, reader := range e.Readers {
		mds[i] = &metadata.Metadata{}
		if err := util.ReadObject(reader, mds[i]); err != nil {
			return err
		}
	}
	leftReader, rightReader := e.Readers[0], e.Readers[1]
	leftMd, rightMd := mds[0], mds[1]

	//write md
	if err := util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	leftRbReader, rightRbReader := row.NewRowsBuffer(leftMd, leftReader, nil), row.NewRowsBuffer(rightMd, rightReader, nil)
	rbWriter := row.NewRowsBuffer(job.Metadata, nil, writer)

	//init
	if err := job.JoinCriteria.Init(job.Metadata); err != nil {
		return err
	}

	//write rows
	rs := make([]*row.Row, 0)
	switch job.JoinType {
	case planner.InnerJoin:
		fallthrough
	case planner.LeftJoin:
		for {
			r, err := rightRbReader.ReadRow()
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return err
			}
			rs = append(rs, r)
		}

		for {
			r, err := leftRbReader.ReadRow()
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return err
			}
			joinNum := 0
			for _, rightRow := range rs {
				joinRow := row.NewRow(r.Vals...)
				joinRow.AppendVals(rightRow.Vals...)
				rg := row.NewRowsGroup(job.Metadata)
				rg.Write(joinRow)
				if ok, err := job.JoinCriteria.Result(rg); ok && err == nil {
					if err = rbWriter.WriteRow(joinRow); err != nil {
						return err
					}
					joinNum++
				} else if err != nil {
					return err
				}
			}
			if job.JoinType == planner.LeftJoin && joinNum == 0 {
				joinRow := row.NewRow(r.Vals...)
				joinRow.AppendVals(make([]interface{}, len(mds[1].GetColumnNames()))...)
				if err = rbWriter.WriteRow(joinRow); err != nil {
					return err
				}
			}
		}

	case planner.RightJoin:
	}

	if err := rbWriter.Flush(); err != nil {
		return err
	}

	return nil
}
