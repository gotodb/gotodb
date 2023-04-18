package executor

import (
	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
	"sync"
)

func (e *Executor) SetInstructionScan(instruction *pb.Instruction) error {
	var job stage.ScanJob
	if err := msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}

	//partition info must decode firstly
	if err := job.Partition.Decode(); err != nil {
		return err
	}

	e.StageJob = &job
	return nil
}

func (e *Executor) RunScan() error {

	job := e.StageJob.(*stage.ScanJob)

	ctr, err := connector.NewConnector(job.Catalog, job.Schema, job.Table)
	if err != nil {
		return err
	}

	//send metadata
	for i := 0; i < len(e.Writers); i++ {
		if err = util.WriteObject(e.Writers[i], job.Metadata); err != nil {
			return err
		}
	}

	colIndexes := job.Metadata.GetColumnIndexes()
	inputMetadata := job.Metadata

	rbWriters := make([]*row.RowsBuffer, len(e.Writers))
	for i, writer := range e.Writers {
		rbWriters[i] = row.NewRowsBuffer(job.Metadata, nil, writer)
	}

	for _, filter := range job.Filters {
		if err := filter.Init(job.Metadata); err != nil {
			return err
		}
	}

	//send rows
	jobs := make(chan *row.RowsGroup)
	var wg sync.WaitGroup

	for _, rbWriter := range rbWriters {
		wg.Add(1)
		go func(rbWriter *row.RowsBuffer) {
			defer func() {
				wg.Done()
			}()

			for {
				rg, ok := <-jobs
				if ok {
					for _, filter := range job.Filters { //TODO: improve performance, add flag in RowsGroup?
						iFlags, err := filter.Result(rg)
						if err != nil {
							e.AddLogInfo(err, pb.LogLevel_ERR)
							break
						}
						rgTmp := row.NewRowsGroup(job.Metadata)
						for i, flag := range iFlags.([]interface{}) {
							if flag.(bool) {
								rgTmp.AppendRowVals(rg.GetRowVals(i)...)
							}
						}
						rg = rgTmp
					}

					if err := rbWriter.Write(rg); err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						break
					}

				} else {
					break
				}
			}
		}(rbWriter)
	}

	// no partitions
	if !job.Partition.IsPartition() {
		for _, file := range job.Partition.GetNoPartitionFiles() {
			var reader row.GroupReader
			reader, err = ctr.GetReader(file, inputMetadata, job.Filters)
			if err != nil {
				break
			}
			for err == nil {
				var rg *row.RowsGroup
				rg, err = reader(colIndexes)
				if err == io.EOF {
					err = nil
					break
				}
				if err != nil {
					break
				}

				jobs <- rg

			}
		}
	} else { // partitioned
		parColNum := job.Partition.GetPartitionColumnNum()
		totColNum := inputMetadata.GetColumnNumber()
		dataColNum := totColNum - parColNum
		var dataCols []int
		var parCols []int

		for _, index := range colIndexes {
			if index < dataColNum {
				dataCols = append(dataCols, index) //column from input
			} else {
				parCols = append(parCols, index-dataColNum) //column from partition
			}
		}
		parMD := inputMetadata.SelectColumnsByIndexes(parCols)

		for i := totColNum - 1; i >= dataColNum; i-- {
			inputMetadata.DeleteColumnByIndex(i)
		}

		for i := 0; i < job.Partition.GetPartitionNum(); i++ {
			parFullRow := job.Partition.GetPartitionRow(i)
			parRow := row.NewRow()
			for _, index := range parCols {
				parRow.AppendVals(parFullRow.Vals[index])
			}

			for _, file := range job.Partition.GetPartitionFiles(i) {
				var reader row.GroupReader
				reader, err = ctr.GetReader(file, inputMetadata, job.Filters)
				if err != nil {
					break
				}
				for err == nil {
					var dataRG *row.RowsGroup
					dataRG, err = reader(dataCols)
					if err == io.EOF {
						err = nil
						break
					}
					if err != nil {
						break
					}

					parRG := row.NewRowsGroup(parMD)
					for i := 0; i < dataRG.GetRowsNumber(); i++ {
						parRG.Write(parRow)
					}

					rg := row.NewRowsGroup(job.Metadata)
					rg.ClearColumns()
					rg.AppendValColumns(dataRG.Vals...)
					rg.AppendValColumns(parRG.Vals...)

					jobs <- rg
				}
			}
		}
	}
	close(jobs)
	wg.Wait()
	if err != nil {
		return err
	}
	for _, rbWriter := range rbWriters {
		if err = rbWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}
