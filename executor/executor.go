package executor

import (
	"context"
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	"github.com/vmihailenco/msgpack"
	"io"
	"net"
	"sync"
)

type Executor struct {
	sync.Mutex
	Name string

	Instruction    *pb.Instruction
	StageJob       stage.Job
	OutputConnChan []chan net.Conn
	Readers        []io.Reader
	Writers        []io.Writer

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
		_ = writer.(io.WriteCloser).Close()
	}
	e.IsStatusChanged = true
	if e.Status != pb.TaskStatus_ERROR {
		e.Status = pb.TaskStatus_SUCCEED
	}
}

func (e *Executor) SendInstruction(ctx context.Context, instruction *pb.Instruction) (*pb.Empty, error) {
	res := &pb.Empty{}

	var runtime config.Runtime
	if err := msgpack.Unmarshal(instruction.RuntimeBytes, &runtime); err != nil {
		return res, err
	}
	config.Conf.Runtime = &runtime

	nodeType := stage.JobType(instruction.TaskType)
	logger.Infof("Instruction: %s", nodeType)
	e.Instruction = instruction
	e.Status = pb.TaskStatus_RUNNING
	e.IsStatusChanged = true
	switch nodeType {
	case stage.JobTypeScan:
		return res, e.SetInstructionScan(instruction)
	case stage.JobTypeSelect:
		return res, e.SetInstructionSelect(instruction)
	case stage.JobTypeGroupBy:
		return res, e.SetInstructionGroupBy(instruction)
	case stage.JobTypeJoin:
		return res, e.SetInstructionJoin(instruction)
	case stage.JobTypeHashJoin:
		return res, e.SetInstructionHashJoin(instruction)
	case stage.JobTypeShuffle:
		return res, e.SetInstructionShuffle(instruction)
	case stage.JobTypeDuplicate:
		return res, e.SetInstructionDuplicate(instruction)
	case stage.JobTypeAggregate:
		return res, e.SetInstructionAggregate(instruction)
	case stage.JobTypeAggregateFuncGlobal:
		return res, e.SetInstructionAggregateFuncGlobal(instruction)
	case stage.JobTypeAggregateFuncLocal:
		return res, e.SetInstructionAggregateFuncLocal(instruction)
	case stage.JobTypeLimit:
		return res, e.SetInstructionLimit(instruction)
	case stage.JobTypeFilter:
		return res, e.SetInstructionFilter(instruction)
	case stage.JobTypeUnion:
		return res, e.SetInstructionUnion(instruction)
	case stage.JobTypeOrderByLocal:
		return res, e.SetInstructionOrderByLocal(instruction)
	case stage.JobTypeOrderBy:
		return res, e.SetInstructionOrderBy(instruction)
	case stage.JobTypeShow:
		return res, e.SetInstructionShow(instruction)
	case stage.JobTypeBalance:
		return res, e.SetInstructionBalance(instruction)
	case stage.JobTypeDistinctLocal:
		return res, e.SetInstructionDistinctLocal(instruction)
	case stage.JobTypeDistinctGlobal:
		return res, e.SetInstructionDistinctGlobal(instruction)
	default:
		e.Status = pb.TaskStatus_TODO
		return res, fmt.Errorf("unknown node type")
	}
}

func (e *Executor) SetupWriters(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	logger.Infof("SetupWriters start")
	var err error

	for range e.StageJob.GetOutputs() {
		pr, pw := io.Pipe()
		e.Writers = append(e.Writers, pw)
		outputConnChan := make(chan net.Conn)
		e.OutputConnChan = append(e.OutputConnChan, outputConnChan)

		go func() {
			w := <-outputConnChan
			err := util.CopyBuffer(pr, w)
			if err != nil && err != io.EOF {
				logger.Errorf("failed to CopyBuffer: %v", err)
			}
			if wc, ok := w.(io.WriteCloser); ok {
				_ = wc.Close()
			}
		}()
	}

	logger.Infof("SetupWriters Output=%v", e.StageJob.GetOutputs())
	return empty, err
}

func (e *Executor) SetupReaders(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	var err error
	logger.Infof("SetupReaders start")
	for _, location := range e.StageJob.GetInputs() {
		pr, pw := io.Pipe()
		e.Readers = append(e.Readers, pr)
		conn, err := net.Dial("tcp", location.GetURL())
		if err != nil {
			logger.Errorf("failed to connect to input channel %v: %v", location, err)
			return empty, err
		}
		logger.Infof("connect to %v", location)
		bytes, _ := msgpack.Marshal(location)

		if _, err := conn.Write(bytes); err != nil {
			logger.Errorf("failed to write to input channel %v: %v", location, err)
			return empty, err
		}

		go func(r io.Reader) {
			err := util.CopyBuffer(r, pw)
			if err != nil && err != io.EOF {
				logger.Errorf("failed to CopyBuffer: %v", err)
			}
			_ = pw.Close()
			if rc, ok := r.(io.ReadCloser); ok {
				_ = rc.Close()
			}
		}(conn)
	}

	logger.Infof("SetupReaders Input=%v", e.StageJob.GetInputs())
	return empty, err
}

func (e *Executor) Run(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	res := &pb.Empty{}
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
	return res, nil
}
