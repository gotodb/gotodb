package executor

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
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

func TestPlan(t *testing.T) {
	sqlStr := "select var1 from test.test.csv limit 10"
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

	aggJob, err := stage.CreateJob(logicalTree, &stageJobs, executorHeap, 1)
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
		exec.SendInstruction(context.Background(), &instruction)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			exec.SetupWriters(context.Background(), new(pb.Empty))

		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			exec.SetupReaders(context.Background(), new(pb.Empty))
		}()
		exec.Run(context.Background(), new(pb.Empty))
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
