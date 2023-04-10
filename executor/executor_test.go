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
	"github.com/gotodb/gotodb/optimizer"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/plan/operator"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	uuid "github.com/satori/go.uuid"
	"github.com/vmihailenco/msgpack"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// to temp dir
//var tempDir = os.TempDir()

// to current dir
var tempDir = "."

func (e *Executor) setupWriters() {
	logger.Infof("SetupWriters start")
	for _, location := range e.StageJob.GetOutputs() {
		file, _ := os.Create(fmt.Sprintf("%s/%s.txt", tempDir, location.Name))
		e.Writers = append(e.Writers, file)
	}

	logger.Infof("SetupWriters Output=%v", e.StageJob.GetOutputs())
}

func (e *Executor) setupReaders() {
	logger.Infof("SetupReaders start")
	for _, location := range e.StageJob.GetInputs() {
		file, err := os.Open(fmt.Sprintf("%s/%s.txt", tempDir, location.Name))
		if err != nil {
			logger.Errorf("failed to open file: %v", err)
		}
		e.Readers = append(e.Readers, file)
	}

	logger.Infof("SetupReaders Input=%v", e.StageJob.GetInputs())
}

func TestExecutor(t *testing.T) {
	config.Load("../config.yaml")
	//sqlStr := "/*+par=1*/select * from file.info.student"
	sqlStr := "/*+partition_number=1*/select * from http.toutiao.info where options = '{ \"url\": \"http://127.0.0.1:2379/v2/keys/queue?recursive=true&sorted=true\", \"dataPath\": \"node.nodes\" }'"
	//sqlStr := "select sum(a.var1), a.var2, a.data_source from test.test.csv as a limit 10"
	//sqlStr := "show Schemas from file"
	//sqlStr := "show tables from file.info"
	//sqlStr := "show Columns from file.info.student"
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

	runtime := config.NewRuntime()
	logicalTree := plan.NewNodeFromSingleStatement(runtime, tree)

	//SetMetaData
	if err := logicalTree.SetMetadata(); err != nil {
		t.Error(err)
		return
	}

	if err := optimizer.DeleteRenameNode(logicalTree); err != nil {
		t.Error(err)
		return
	}

	if err := optimizer.FilterColumns(logicalTree, []string{}); err != nil {
		t.Error(err)
		return
	}

	if err := optimizer.PredicatePushDown(logicalTree, []*operator.BooleanExpressionNode{}); err != nil {
		t.Error(err)
		return
	}

	if err := optimizer.ExtractAggFunc(logicalTree); err != nil {
		t.Error(err)
		return
	}

	executorHeap := util.NewHeap()
	heap.Init(executorHeap)
	for i := 0; i < 100; i++ {
		heap.Push(executorHeap, util.NewItem(&pb.Location{Name: fmt.Sprintf("%v", i)}, 1))
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
			TaskID:               taskId,
			TaskType:             int32(job.GetType()),
			EncodedStageJobBytes: buf,
			RuntimeBytes:         runtimeBuf,
			Location:             loc,
		}
		exec := New(loc.Name)

		if _, err := exec.SendInstruction(context.Background(), &instruction); err != nil {
			t.Errorf("exec.SendInstruction: %v", err)
			return
		}

		exec.setupWriters()
		exec.setupReaders()

		if _, err := exec.Run(context.Background(), new(pb.Empty)); err != nil {
			t.Errorf("exec.Run: %v", err)
			return
		}
		for {
			if exec.Status == pb.TaskStatus_SUCCEED {
				t.Logf("Done job %s", exec.StageJob.GetType())
				break
			} else if exec.Status == pb.TaskStatus_ERROR {
				for _, info := range exec.Infos {
					t.Errorf("info %s", info)
				}
				return
			} else {
				// In order to switch coroutines
				time.Sleep(100 * time.Millisecond)
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
