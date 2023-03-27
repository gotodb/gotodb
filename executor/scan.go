package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/stage"
	"io"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionScan(instruction *pb.Instruction) error {
	logger.Infof("set instruction scan")

	var job stage.ScanJob
	var err error
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}
	job.PartitionInfo.Decode() //partition info must decode firstly

	e.StageJob = &job
	e.Instruction = instruction
	for i := 0; i < len(job.Outputs); i++ {
		loc := job.Outputs[i]
		e.OutputLocations = append(e.OutputLocations, &loc)
	}
	return nil
}

func (e *Executor) RunScan() (err error) {
	f, _ := os.Create(fmt.Sprintf("executor_%v_scan_%v_cpu.pprof", e.Name, time.Now().Format("20060102150405")))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		for i := 0; i < len(e.Writers); i++ {
			util.WriteEOFMessage(e.Writers[i])
			e.Writers[i].(io.WriteCloser).Close()
		}
		if err != nil {
			e.AddLogInfo(err, pb.LogLevel_ERR)
		}
		e.Clear()

	}()

	if e.Instruction == nil {
		return fmt.Errorf("No Instruction")
	}

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

	var colIndexes []int
	inputMetadata := job.InputMetadata
	for _, c := range job.Metadata.Columns {
		cn := c.ColumnName
		index, err := inputMetadata.GetIndexByName(cn)
		if err != nil {
			return err
		}
		colIndexes = append(colIndexes, index)
	}

	rbWriters := make([]*row.RowsBuffer, len(e.Writers))
	for i, writer := range e.Writers {
		rbWriters[i] = row.NewRowsBuffer(job.Metadata, nil, writer)
	}

	defer func() {
		for _, rbWriter := range rbWriters {
			rbWriter.Flush()
		}
	}()

	//init
	for _, filter := range job.Filters {
		if err := filter.Init(job.Metadata); err != nil {
			return err
		}
	}

	//send rows
	jobs := make(chan *row.RowsGroup)
	var wg sync.WaitGroup

	for i := 0; i < int(config.Conf.Runtime.ParallelNumber); i++ {
		wg.Add(1)
		go func(parallelNumber int) {
			defer func() {
				wg.Done()
			}()

			k := parallelNumber % len(e.Writers)

			for {
				rg, ok := <-jobs
				if ok {
					for _, filter := range job.Filters { //TODO: improve performance, add flag in RowsGroup?
						flagsi, err := filter.Result(rg)
						if err != nil {
							e.AddLogInfo(err, pb.LogLevel_ERR)
							break
						}
						flags := flagsi.([]interface{})
						rgtmp := row.NewRowsGroup(job.Metadata)

						for i, flag := range flags {
							if flag.(bool) {
								rgtmp.AppendRowVals(rg.GetRowVals(i)...)
							}
						}
						rg = rgtmp
					}

					if err := rbWriters[k].Write(rg); err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						break
					}
					k++
					k = k % len(e.Writers)

				} else {
					break
				}
			}
		}(i)
	}

	// no partitions
	if !job.PartitionInfo.IsPartition() {
		for _, file := range job.PartitionInfo.GetNoPartititonFiles() {
			reader := ctr.GetReader(file, inputMetadata)
			if err != nil {
				break
			}
			for err == nil {
				rg, err := reader(colIndexes)
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
		parColNum := job.PartitionInfo.GetPartitionColumnNum()
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

		for i := 0; i < job.PartitionInfo.GetPartitionNum(); i++ {
			parFullRow := job.PartitionInfo.GetPartitionRow(i)
			parRow := row.NewRow()
			for _, index := range parCols {
				parRow.AppendVals(parFullRow.Vals[index])
			}

			for _, file := range job.PartitionInfo.GetPartitionFiles(i) {
				reader := ctr.GetReader(file, inputMetadata)
				if err != nil {
					break
				}
				for err == nil {
					dataRG, err := reader(dataCols)
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

	logger.Infof("RunScan finished")
	return err
}
