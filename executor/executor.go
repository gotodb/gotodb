package executor

import (
	"context"
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/stage"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack"
	"io"
	"net"
	"sync"
)

type Executor struct {
	sync.Mutex
	Name string

	Instruction *pb.Instruction
	StageJob    stage.Job
	Readers     []io.Reader
	Writers     []io.Writer

	Status          pb.TaskStatus
	IsStatusChanged bool
	Infos           []*pb.LogInfo
}

var executors = make(map[string]*Executor)

func New(name string) *Executor {
	res := &Executor{
		Name:   name,
		Infos:  []*pb.LogInfo{},
		Status: pb.TaskStatus_TODO,
	}
	executors[name] = res
	return res
}

func Get(name string) (*Executor, error) {
	if exec, ok := executors[name]; !ok {
		return nil, fmt.Errorf("executor not exists: %s", name)
	} else {
		return exec, nil
	}
}

func Delete(name string) {
	delete(executors, name)
}

func (e *Executor) AddLogInfo(info interface{}, level pb.LogLevel) {
	if info == nil {
		return
	}
	logInfo := &pb.LogInfo{
		Level: level,
		Info:  []byte(fmt.Sprintf("%v", info)),
	}
	e.Lock()
	defer e.Unlock()
	e.Infos = append(e.Infos, logInfo)
	if level == pb.LogLevel_ERR {
		e.Status = pb.TaskStatus_ERROR
		e.IsStatusChanged = true
	}
}

func (e *Executor) Clear() {
	for _, writer := range e.Writers {
		switch w := writer.(type) {
		case io.WriteCloser:
			_ = w.Close()
		case net.Conn:
			_ = w.Close()
		}
	}
	e.IsStatusChanged = true
	if e.Status != pb.TaskStatus_ERROR {
		e.Status = pb.TaskStatus_SUCCEED
	}
}

func (e *Executor) SendInstruction(instruction *pb.Instruction) error {
	var runtime config.Runtime
	if err := msgpack.Unmarshal(instruction.RuntimeBytes, &runtime); err != nil {
		return err
	}
	config.Conf.Runtime = &runtime

	nodeType := stage.JobType(instruction.TaskType)
	e.Instruction = instruction
	e.Status = pb.TaskStatus_RUNNING
	e.IsStatusChanged = true
	logrus.Infof("%s instruction: %s", e.Instruction.TaskID, nodeType)
	var err error
	switch nodeType {
	case stage.JobTypeScan:
		err = e.SetInstructionScan(instruction)
	case stage.JobTypeSelect:
		err = e.SetInstructionSelect(instruction)
	case stage.JobTypeGroupBy:
		err = e.SetInstructionGroupBy(instruction)
	case stage.JobTypeJoin:
		err = e.SetInstructionJoin(instruction)
	case stage.JobTypeHashJoin:
		err = e.SetInstructionHashJoin(instruction)
	case stage.JobTypeShuffle:
		err = e.SetInstructionShuffle(instruction)
	case stage.JobTypeDuplicate:
		err = e.SetInstructionDuplicate(instruction)
	case stage.JobTypeAggregate:
		err = e.SetInstructionAggregate(instruction)
	case stage.JobTypeAggregateFuncGlobal:
		err = e.SetInstructionAggregateFuncGlobal(instruction)
	case stage.JobTypeAggregateFuncLocal:
		err = e.SetInstructionAggregateFuncLocal(instruction)
	case stage.JobTypeLimit:
		err = e.SetInstructionLimit(instruction)
	case stage.JobTypeFilter:
		err = e.SetInstructionFilter(instruction)
	case stage.JobTypeUnion:
		err = e.SetInstructionUnion(instruction)
	case stage.JobTypeOrderByLocal:
		err = e.SetInstructionOrderByLocal(instruction)
	case stage.JobTypeOrderBy:
		err = e.SetInstructionOrderBy(instruction)
	case stage.JobTypeShow:
		err = e.SetInstructionShow(instruction)
	case stage.JobTypeBalance:
		err = e.SetInstructionBalance(instruction)
	case stage.JobTypeDistinctLocal:
		err = e.SetInstructionDistinctLocal(instruction)
	case stage.JobTypeDistinctGlobal:
		err = e.SetInstructionDistinctGlobal(instruction)
	default:
		e.Status = pb.TaskStatus_TODO
		err = fmt.Errorf("unknown node type")
	}

	return err
}

func (e *Executor) SetupPipe() error {
	e.Writers = make([]io.Writer, len(e.StageJob.GetOutputs()))
	for _, location := range e.StageJob.GetInputs() {
		conn, err := net.Dial("tcp", location.GetURL())
		if err != nil {
			logrus.Errorf("%s failed to connect to input channel %v: %v", e.Instruction.TaskID, location, err)
			return err
		}
		logrus.Infof("connect to %v", location)
		bytes, _ := msgpack.Marshal(location)
		if _, err := conn.Write(bytes); err != nil {
			logrus.Errorf("%s failed to write to input channel %v: %v", e.Instruction.TaskID, location, err)
			return err
		}
		e.Readers = append(e.Readers, conn)
	}
	logrus.Infof("%s setup readers Input=%v", e.Instruction.TaskID, e.StageJob.GetInputs())
	return nil
}

func (e *Executor) Run(ctx context.Context) error {
	nodeType := stage.JobType(e.Instruction.TaskType)
	var err error
	defer func() {
		//pprof.StopCPUProfile()
		e.AddLogInfo(err, pb.LogLevel_ERR)
		e.Clear()
	}()
	//f, err := os.Create(fmt.Sprintf("executor_%v_%d_%v_cpu.pprof", e.Name, nodeType, time.Now().Format("20060102150405")))
	//if err != nil {
	//	return res, err
	//}

	//err = pprof.StartCPUProfile(f)
	//if err != nil {
	//	return res, err
	//}

	switch nodeType {
	case stage.JobTypeScan:
		err = e.RunScan()
	case stage.JobTypeSelect:
		err = e.RunSelect()
	case stage.JobTypeGroupBy:
		err = e.RunGroupBy()
	case stage.JobTypeJoin:
		err = e.RunJoin()
	case stage.JobTypeHashJoin:
		err = e.RunHashJoin()
	case stage.JobTypeShuffle:
		err = e.RunShuffle()
	case stage.JobTypeDuplicate:
		err = e.RunDuplicate()
	case stage.JobTypeAggregate:
		err = e.RunAggregate()
	case stage.JobTypeAggregateFuncGlobal:
		err = e.RunAggregateFuncGlobal()
	case stage.JobTypeAggregateFuncLocal:
		err = e.RunAggregateFuncLocal()
	case stage.JobTypeLimit:
		err = e.RunLimit()
	case stage.JobTypeFilter:
		err = e.RunFilter()
	case stage.JobTypeOrderByLocal:
		err = e.RunOrderByLocal()
	case stage.JobTypeOrderBy:
		err = e.RunOrderBy()
	case stage.JobTypeUnion:
		err = e.RunUnion()
	case stage.JobTypeShow:
		err = e.RunShow()
	case stage.JobTypeBalance:
		err = e.RunBalance()
	case stage.JobTypeDistinctLocal:
		err = e.RunDistinctLocal()
	case stage.JobTypeDistinctGlobal:
		err = e.RunDistinctGlobal()
	default:
		err = fmt.Errorf("unknown job type")
	}

	if err != nil {
		logrus.Infof("%s run %s error: %v", e.Instruction.TaskID, nodeType, err)
		return err
	}

	logrus.Infof("%s run %s finished", e.Instruction.TaskID, nodeType)

	return nil
}
