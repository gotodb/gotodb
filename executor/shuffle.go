package executor

import (
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
	"sync"
)

func (e *Executor) SetInstructionShuffle(instruction *pb.Instruction) error {
	var job stage.ShuffleJob
	if err := msgpack.Unmarshal(instruction.EncodedStageJobBytes, &job); err != nil {
		return err
	}
	e.StageJob = &job
	return nil
}

func ShuffleHash(s string) int {
	res := 0
	for _, c := range []byte(s) {
		res += int(c)
	}
	return res
}

func (e *Executor) RunShuffle() error {
	job := e.StageJob.(*stage.ShuffleJob)
	//read md
	md := &metadata.Metadata{}
	for _, reader := range e.Readers {
		if err := util.ReadObject(reader, md); err != nil {
			return err
		}
	}

	mdOutput := md.Copy()

	//write md
	if job.Keys != nil && len(job.Keys) > 0 {
		mdOutput.ClearKeys()
		mdOutput.AppendKeyByType(datatype.STRING)
	}
	for _, writer := range e.Writers {
		if err := util.WriteObject(writer, mdOutput); err != nil {
			return err
		}
	}

	rbWriters := make([]*row.RowsBuffer, len(e.Writers))
	for i, writer := range e.Writers {
		rbWriters[i] = row.NewRowsBuffer(mdOutput, nil, writer)
	}

	//init
	for _, k := range job.Keys {
		if err := k.Init(md); err != nil {
			return err
		}
	}

	//write rows
	var wg sync.WaitGroup
	for index := range e.Readers {
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

				for rowNumber := 0; rowNumber < rg0.GetRowsNumber(); rowNumber++ {
					r := rg0.GetRow(rowNumber)
					i := 0
					if job.Keys != nil && len(job.Keys) > 0 {
						rg := row.NewRowsGroup(mdOutput)
						rg.Write(r)
						key, err := CalHashKey(job.Keys, rg)
						if err != nil {
							e.AddLogInfo(err, pb.LogLevel_ERR)
							return
						}
						r.AppendKeys(key)
						i = ShuffleHash(key) % len(rbWriters)
					}

					if err = rbWriters[i].WriteRow(r); err != nil {
						e.AddLogInfo(err, pb.LogLevel_ERR)
						return
					}

					row.Pool.Put(r)
				}
			}
		}(index)
	}

	wg.Wait()

	for _, rbWriter := range rbWriters {
		if err := rbWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}
