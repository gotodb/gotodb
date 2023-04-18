package executor

import (
	"github.com/gotodb/gotodb/stage"
	"io"

	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionShow(instruction *pb.Instruction) error {
	var job stage.ShowJob
	if err := msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func (e *Executor) RunShow() error {
	job := e.StageJob.(*stage.ShowJob)

	md := job.Metadata
	writer := e.Writers[0]
	//write metadata
	if err := util.WriteObject(writer, md); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(md, nil, writer)

	ctr := connector.NewEmptyConnector(job.Catalog)
	var showReader func() (*row.Row, error)
	//writer rows
	switch job.ShowType {
	case plan.ShowCatalogs:
	case plan.ShowSchemas:
		showReader = ctr.ShowSchemas(job.Catalog, job.LikePattern, job.Escape)
	case plan.ShowTables:
		showReader = ctr.ShowTables(job.Catalog, job.Schema, job.LikePattern, job.Escape)
	case plan.ShowColumns:
		showReader = ctr.ShowColumns(job.Catalog, job.Schema, job.Table)
	case plan.ShowPartitions:
		showReader = ctr.ShowPartitions(job.Catalog, job.Schema, job.Table)
	}

	for {
		r, err := showReader()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return err
		}

		if err = rbWriter.WriteRow(r); err != nil {
			return err
		}
	}

	if err := rbWriter.Flush(); err != nil {
		return err
	}

	return nil

}
