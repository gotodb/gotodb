package executor

import (
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
)

func (e *Executor) SetInstructionAggregateFuncLocal(instruction *pb.Instruction) (err error) {
	var job stage.AggregateFuncLocalJob
	if err = msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job

	return nil
}

func (e *Executor) RunAggregateFuncLocal() (err error) {
	reader, writer := e.Readers[0], e.Writers[0]
	job := e.StageJob.(*stage.AggregateFuncLocalJob)
	md := &metadata.Metadata{}

	//read md
	if err = util.ReadObject(reader, md); err != nil {
		return err
	}

	//write md
	if err = util.WriteObject(writer, job.Metadata); err != nil {
		return err
	}

	rbReader, rbWriter := row.NewRowsBuffer(md, reader, nil), row.NewRowsBuffer(job.Metadata, nil, writer)

	defer func() {
		rbWriter.Flush()
	}()

	//init
	if err := job.Init(job.Metadata); err != nil {
		return err
	}

	//write rows
	var rg *row.RowsGroup
	res := make([]map[string]interface{}, len(job.FuncNodes))
	for i := 0; i < len(job.FuncNodes); i++ {
		res[i] = map[string]interface{}{}
	}

	keys := map[string]*row.Row{}
	for {
		rg, err = rbReader.Read()

		if err == io.EOF {
			err = nil

			for key, r := range keys {
				for i := 0; i < len(res); i++ {
					r.AppendVals(res[i][key])
				}
				rbWriter.WriteRow(r)
			}

			break
		}
		if err != nil {
			break
		}

		for i := 0; i < rg.GetRowsNumber(); i++ {
			key := rg.GetKeyString(i)
			if _, ok := keys[key]; !ok {
				keys[key] = rg.GetRow(i)
			}
		}

		if err = e.CalAggregateFuncLocal(job, rg, &res); err != nil {
			break
		}
	}

	logger.Infof("RunAggregateFuncLocal finished")
	return err
}

func (e *Executor) CalAggregateFuncLocal(job *stage.AggregateFuncLocalJob, rg *row.RowsGroup, res *[]map[string]interface{}) error {
	var err error
	var resc map[string]interface{}
	var resci interface{}
	for i, item := range job.FuncNodes {
		if resci, err = item.Result(rg); err != nil {
			break
		}
		resc = resci.(map[string]interface{})
		for k, v := range resc {
			(*res)[i][k] = v
		}

	}
	return err
}
