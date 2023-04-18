package executor

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
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
	"github.com/sirupsen/logrus"
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
	logrus.Infof("SetupWriters start")
	e.Writers = []io.Writer{}
	for _, location := range e.StageJob.GetOutputs() {
		file, _ := os.Create(fmt.Sprintf("%s/%s-%d.txt", tempDir, location.Name, location.ChannelIndex))
		e.Writers = append(e.Writers, file)
	}

	logrus.Infof("SetupWriters Output=%v", e.StageJob.GetOutputs())
}

func (e *Executor) setupReaders() {
	logrus.Infof("SetupReaders start")
	e.Readers = []io.Reader{}
	for _, location := range e.StageJob.GetInputs() {
		file, err := os.Open(fmt.Sprintf("%s/%s-%d.txt", tempDir, location.Name, location.ChannelIndex))
		if err != nil {
			logrus.Errorf("failed to open file: %v", err)
		}
		e.Readers = append(e.Readers, file)
	}

	logrus.Infof("SetupReaders Input=%v", e.StageJob.GetInputs())
}

func TestSelect(t *testing.T) {
	sql := "/*+partition_number=1*/select * from http.etcd.info where _http = '{ \"url\": \"http://127.0.0.1:2379/v2/keys/queue\", \"uri\": \"recursive=true&sorted=true\", \"dataPath\": \"node.nodes\", \"timeout\": 2000 }' group by value"
	//sql := "/*+partition_number=1*/select a.* from mysql.goploy.user as a join mysql.goploy.user as b on a.id = b.id where a.id = 1 and b.id = 2"
	//sql := "select a.id, b.id from mysql.goploy.user as a join file.info.student as b on a.id = b.id  where a.id = 1"
	//sql := "select id from mysql.goploy.user union select id from file.info.student where id = 4"
	//sql := "select name from file.info.student"
	//sql := "select id from mysql.goploy.user"
	//sql := "select  * from test.test.csv as a limit 10"
	executor(t, sql)
}

func TestShowSchemas(t *testing.T) {
	sql := "show Schemas from mysql"
	executor(t, sql)
}

func TestShowTables(t *testing.T) {
	sql := "show tables from mysql.goploy"
	executor(t, sql)
}

func TestShowColumns(t *testing.T) {
	sql := "show Columns from mysql.goploy.user"
	executor(t, sql)
}

func TestShowPartitions(t *testing.T) {
	sql := "show partitions from file.info.student"
	executor(t, sql)
}

func executor(t *testing.T, sqlStr string) {
	config.Load("../config.yaml")
	//sqlStr := "/*+par=1*/select * from file.info.student"
	//sqlStr := "select sum(a.var1), a.var2, a.data_source from test.test.csv as a limit 10"
	hint := optimizer.ParseHint(sqlStr)
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

	partitionNumber := config.Conf.Runtime.ParallelNumber
	if hint.PartitionNumber > 0 {
		partitionNumber = hint.PartitionNumber
	}

	stageJobs, err := stage.CreateJob(logicalTree, executorHeap, partitionNumber)
	if err != nil {
		t.Error(err)
		return
	}

	var (
		buf        []byte
		runtimeBuf []byte
		taskId     = fmt.Sprintf("%v_%v", time.Now().Format("20060102150405"), uuid.NewV4().String())
		aggJob     stage.Job
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

		if err := exec.SendInstruction(&instruction); err != nil {
			t.Errorf("exec.SendInstruction: %v", err)
			return
		}

		exec.setupWriters()
		exec.setupReaders()

		if err := exec.Run(context.Background()); err != nil {
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
		aggJob = job
	}
	var (
		msg []byte
		r   *row.Row
	)
	md := &metadata.Metadata{}
	aggLoc := aggJob.GetLocation()
	file, err := os.Open(fmt.Sprintf("%s/%s-%d.txt", tempDir, aggLoc.Name, aggLoc.ChannelIndex))
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
