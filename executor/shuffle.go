package executor

import (
	"fmt"
	"github.com/gotodb/gotodb/stage"
	"io"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetInstructionShuffle(instruction *pb.Instruction) (err error) {
	var job stage.ShuffleJob
	if err = msgpack.Unmarshal(instruction.EncodedEPlanNodeBytes, &job); err != nil {
		return err
	}
	e.Instruction = instruction
	e.StageJob = &job
	e.InputLocations = []*pb.Location{}
	for i := 0; i < len(job.Inputs); i++ {
		loc := job.Inputs[i]
		e.InputLocations = append(e.InputLocations, &loc)
	}
	e.OutputLocations = []*pb.Location{}
	for i := 0; i < len(job.Outputs); i++ {
		loc := job.Outputs[i]
		e.OutputLocations = append(e.OutputLocations, &loc)
	}
	return nil
}

func ShuffleHash(s string) int {
	res := 0
	for _, c := range []byte(s) {
		res += int(c)
	}
	return res
}

func (e *Executor) RunShuffle() (err error) {
	f, _ := os.Create(fmt.Sprintf("executor_%v_hashjoinshuffle_%v_cpu.pprof", e.Name, time.Now().Format("20060102150405")))
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	defer func() {
		if err != nil {
			e.AddLogInfo(err, pb.LogLevel_ERR)
		}
		e.Clear()
	}()
	job := e.StageJob.(*stage.ShuffleJob)
	//read md
	md := &metadata.Metadata{}
	for _, reader := range e.Readers {
		if err = util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	mdOutput := md.Copy()

	//write md
	if job.Keys != nil && len(job.Keys) > 0 {
		mdOutput.ClearKeys()
		mdOutput.AppendKeyByType(gtype.STRING)
	}
	for _, writer := range e.Writers {
		if err = util.WriteObject(writer, mdOutput); err != nil {
			return err
		}
	}

	rbWriters := make([]*row.RowsBuffer, len(e.Writers))
	for i, writer := range e.Writers {
		rbWriters[i] = row.NewRowsBuffer(mdOutput, nil, writer)
	}

	defer func() {
		for _, rbWriter := range rbWriters {
			rbWriter.Flush()
		}
	}()

	//init
	for _, k := range job.Keys {
		if err := k.Init(md); err != nil {
			return err
		}
	}

	//write rows
	var wg sync.WaitGroup
	for i, _ := range e.Readers {
		wg.Add(1)
		go func(index int) {
			defer func() {
				wg.Done()
			}()
			reader := e.Readers[index]
			rbReader := row.NewRowsBuffer(md, reader, nil)
			for {
				rg0, err := rbReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					e.AddLogInfo(err, pb.LogLevel_ERR)
					return
				}

				for i := 0; i < rg0.GetRowsNumber(); i++ {
					r := rg0.GetRow(i)
					index := 0
					if job.Keys != nil && len(job.Keys) > 0 {
						rg := row.NewRowsGroup(mdOutput)
						rg.Write(r)
						key, err := CalHashKey(job.Keys, rg)
						if err != nil {
							e.AddLogInfo(err, pb.LogLevel_ERR)
							return
						}
						r.AppendKeys(key)
						index = ShuffleHash(key) % len(rbWriters)

					}

					if err = rbWriters[index].WriteRow(r); err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						return
					}

					row.RowPool.Put(r)
				}
			}
		}(i)
	}

	wg.Wait()

	return nil
}
