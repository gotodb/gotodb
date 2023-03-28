package executor

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	uuid "github.com/satori/go.uuid"
	"github.com/vmihailenco/msgpack"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// to temp dir
//var tempDir = os.TempDir()

// to current dir
var tempDir = "."

func (e *Executor) setupWriters() {
	logger.Infof("SetupWriters start")
	var wg sync.WaitGroup
	for i := 0; i < len(e.OutputLocations); i++ {
		pr, pw := io.Pipe()
		e.Writers = append(e.Writers, pw)
		e.OutputChannelLocations = append(e.OutputChannelLocations,
			&pb.Location{
				Name:    e.OutputLocations[i].Name,
				Address: e.OutputLocations[i].Address,
				Port:    e.OutputLocations[i].Port,
			},
		)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			file, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", tempDir, e.OutputLocations[i].Name), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
			if err != nil {
				logger.Errorf("failed to open file: %v", err)
				return
			}

			if _, err := io.Copy(file, pr); err != nil {
				logger.Errorf("writer failed to CopyBuffer: %v", err)
			}

			if err := file.Close(); err != nil {
				logger.Errorf("close writer failed: %v", err)
			}
		}(i)
	}
	wg.Wait()
	logger.Infof("SetupWriters Input=%v, Output=%v", e.InputChannelLocations, e.OutputChannelLocations)
}

func (e *Executor) setupReaders() {
	logger.Infof("SetupReaders start")
	for i := 0; i < len(e.InputLocations); i++ {
		file, err := os.Open(fmt.Sprintf("%s/%s.txt", tempDir, e.InputLocations[i].Name))
		if err != nil {
			logger.Errorf("failed to open file: %v", err)
		}
		e.Readers = append(e.Readers, file)
		e.InputChannelLocations = append(e.InputChannelLocations, &pb.Location{
			Name:    e.InputLocations[i].Name,
			Address: e.InputLocations[i].Address,
			Port:    e.InputLocations[i].Port,
		})
	}

	logger.Infof("SetupReaders Input=%v, Output=%v", e.InputChannelLocations, e.OutputChannelLocations)
}

func TestExecutor(t *testing.T) {
	sqlStr := "select var1, var2, data_source from test.test.csv limit 10"
	inputStream := antlr.NewInputStream(sqlStr)
	lexer := parser.NewSqlLexer(parser.NewCaseChangingStream(inputStream, true))
	p := parser.NewSqlParser(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
	errListener := parser.NewErrorListener()
	p.AddErrorListener(errListener)
	tree := p.SingleStatement()
	if errListener.HasError() {
		t.Error(errListener)
		return
	}

	runtime := config.NewConfigRuntime()
	logicalTree := plan.NewNodeFromSingleStatement(runtime, tree)

	//SetMetaData
	if err := logicalTree.SetMetadata(); err != nil {
		t.Error(err)
		return
	}

	executorHeap := util.NewHeap()
	heap.Init(executorHeap)
	for i := 0; i < 100; i++ {
		heap.Push(executorHeap, util.NewItem(pb.Location{Name: fmt.Sprintf("%v", i)}, 1))
	}
	var stageJobs []stage.Job

	aggJob, err := stage.CreateJob(logicalTree, &stageJobs, executorHeap, 2)
	if err != nil {
		t.Error(err)
		return
	}
	var (
		buf        []byte
		runtimeBuf []byte
		taskId     = fmt.Sprintf("%v_%v", time.Now().Format("20060102150405"), uuid.NewV4().String())
	)

	for _, job := range stageJobs {
		if buf, err = msgpack.Marshal(job); err != nil {
			break
		}

		if runtimeBuf, err = msgpack.Marshal(runtime); err != nil {
			break
		}
		loc := job.GetLocation()
		instruction := pb.Instruction{
			TaskId:                taskId,
			TaskType:              int32(job.GetType()),
			EncodedEPlanNodeBytes: buf,
			RuntimeBytes:          runtimeBuf,
			Location:              &loc,
		}
		exec := NewExecutor("", taskId, "test")

		if _, err := exec.SendInstruction(context.Background(), &instruction); err != nil {
			t.Errorf("exec.SendInstruction: %v", err)
			return
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			exec.setupWriters()

		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			exec.setupReaders()
		}()

		if _, err := exec.Run(context.Background(), new(pb.Empty)); err != nil {
			t.Errorf("exec.Run: %v", err)
			return
		}
		wg.Wait()
		for {
			if exec.Status == pb.TaskStatus_SUCCEED {
				t.Logf("Done job %s", exec.StageJob.GetType())
				break
			} else if exec.Status == pb.TaskStatus_ERROR {
				for _, info := range exec.Infos {
					t.Errorf("info %s", info)
				}
				return
			}
		}
	}
	var (
		msg []byte
		r   *row.Row
	)
	md := &metadata.Metadata{}
	aggLoc := aggJob.GetLocation()
	file, err := os.Open(fmt.Sprintf("%s.txt", aggLoc.Name))
	if err != nil {
		t.Errorf("failed to open file: %v", err)
		return
	}
	if err = util.ReadObject(file, md); err != nil {
		t.Errorf("read md err: %v", err)
		return
	}
	if msg, err = json.MarshalIndent(md, "", "    "); err != nil {
		t.Errorf("json marshal: %v", err)
		return
	}
	msg = append(msg, []byte("\n")...)

	fmt.Println(string(msg))

	rbReader := row.NewRowsBuffer(md, file, nil)

	for {
		r, err = rbReader.ReadRow()

		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			t.Errorf("read line: %v", err)
			return
		}

		var res []string
		for i := 0; i < len(r.Vals); i++ {
			res = append(res, fmt.Sprintf("%v", r.Vals[i]))
		}
		msg = []byte(strings.Join(res, ","))
		msg = append(msg, []byte("\n")...)

		fmt.Println(string(msg))
	}

}
