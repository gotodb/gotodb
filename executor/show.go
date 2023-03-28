package executor

import (
	"github.com/gotodb/gotodb/stage"
	"io"

	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionShow(instruction *pb.Instruction) error {
	var job stage.ShowJob
	var err error
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}

	e.StageJob = &job
	e.Instruction = instruction
	e.InputLocations = []*pb.Location{}
	e.OutputLocations = append(e.OutputLocations, &job.Output)
	return nil
}

func (e *Executor) RunShow() (err error) {
	defer func() {
		for i := 0; i < len(e.Writers); i++ {
			util.WriteEOFMessage(e.Writers[i])
			e.Writers[i].(io.WriteCloser).Close()
		}
	}()

	job := e.StageJob.(*stage.ShowJob)
	ctr, err := connector.NewConnector(job.Catalog, job.Schema, job.Table)
	if err != nil {
		return err
	}

	md := job.Metadata
	writer := e.Writers[0]
	//write metadata
	if err = util.WriteObject(writer, md); err != nil {
		return err
	}

	rbWriter := row.NewRowsBuffer(md, nil, writer)

	var showReader func() (*row.Row, error)
	//writer rows
	switch job.ShowType {
	case plan.SHOWCATALOGS:
	case plan.SHOWSCHEMAS:
		showReader = ctr.ShowSchemas(job.Catalog, job.Schema, job.Table, job.LikePattern, job.Escape)
	case plan.SHOWTABLES:
		showReader = ctr.ShowTables(job.Catalog, job.Schema, job.Table, job.LikePattern, job.Escape)
	case plan.SHOWCOLUMNS:
		showReader = ctr.ShowColumns(job.Catalog, job.Schema, job.Table)
	case plan.SHOWPARTITIONS:
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

	if err = rbWriter.Flush(); err != nil {
		return err
	}

	logger.Infof("RunShowTables finished")
	return err

}
