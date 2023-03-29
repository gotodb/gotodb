package executor

import (
	"context"
	"fmt"
	"github.com/gotodb/gotodb/stage"
	"github.com/vmihailenco/msgpack"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/pb"
	"github.com/kardianos/osext"
	"google.golang.org/grpc"
)

type Executor struct {
	sync.Mutex
	AgentAddress string

	Address string
	Name    string

	Instruction                                   *pb.Instruction
	StageJob                                      stage.Job
	InputLocations, OutputLocations               []*pb.Location
	InputChannelLocations, OutputChannelLocations []*pb.Location
	Readers                                       []io.Reader
	Writers                                       []io.Writer

	Status          pb.TaskStatus
	IsStatusChanged bool
	Infos           []*pb.LogInfo

	DoneChan chan int
}

var executorServer *Executor

func NewExecutor(agentAddress string, address, name string) *Executor {
	res := &Executor{
		AgentAddress: agentAddress,
		Address:      address,
		Name:         name,
		DoneChan:     make(chan int),
		Infos:        []*pb.LogInfo{},
		Status:       pb.TaskStatus_TODO,
	}
	return res
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
		writer.(io.WriteCloser).Close()
	}
	e.IsStatusChanged = true
	if e.Status != pb.TaskStatus_ERROR {
		e.Status = pb.TaskStatus_SUCCEED
	}

	select {
	case <-e.DoneChan:
	default:
		close(e.DoneChan)
	}
}

func (e *Executor) Duplicate(ctx context.Context, em *pb.Empty) (*pb.Empty, error) {
	res := &pb.Empty{}
	exeFullName, _ := osext.Executable()

	command := exec.Command(exeFullName,
		fmt.Sprintf("executor"),
		"--agent",
		fmt.Sprintf("%v", e.AgentAddress),
		"--address",
		fmt.Sprintf("%v", strings.Split(e.Address, ":")[0]+":0"),
		"--config",
		fmt.Sprintf("%v", config.Conf.File),
	)

	command.Stdout = os.Stdout
	command.Stdin = os.Stdin
	command.Stderr = os.Stderr
	err := command.Start()
	return res, err
}

func (e *Executor) Quit(ctx context.Context, em *pb.Empty) (*pb.Empty, error) {
	res := &pb.Empty{}
	os.Exit(0)
	return res, nil
}

func (e *Executor) Restart(ctx context.Context, em *pb.Empty) (*pb.Empty, error) {
	res := &pb.Empty{}
	e.Duplicate(context.Background(), em)
	time.Sleep(time.Second)
	e.Quit(ctx, em)
	return res, nil
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
	e.Status = pb.TaskStatus_RUNNING
	e.IsStatusChanged = true

	e.DoneChan = make(chan int)
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

func (e *Executor) Run(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	res := &pb.Empty{}
	nodeType := stage.JobType(e.Instruction.TaskType)
	go func() {
		var err error
		defer func() {
			pprof.StopCPUProfile()
			e.AddLogInfo(err, pb.LogLevel_ERR)
			e.Clear()
		}()
		f, err := os.Create(fmt.Sprintf("executor_%v_%d_%v_cpu.pprof", e.Name, nodeType, time.Now().Format("20060102150405")))
		if err != nil {
			return
		}

		err = pprof.StartCPUProfile(f)
		if err != nil {
			return
		}

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
	}()
	return res, nil
}

func (e *Executor) GetOutputChannelLocation(ctx context.Context, location *pb.Location) (*pb.Location, error) {
	if int(location.ChannelIndex) >= len(e.OutputChannelLocations) {
		return nil, fmt.Errorf("ChannelLocation %v not found: %v", location.ChannelIndex, location)
	}
	return e.OutputChannelLocations[location.ChannelIndex], nil
}

func RunExecutor(masterAddress string, address, name string) {
	executorServer = NewExecutor(masterAddress, address, name)
	listener, err := net.Listen("tcp", executorServer.Address)
	if err != nil {
		log.Fatalf("Executor failed to run: %v", err)
	}
	defer listener.Close()
	executorServer.Address = listener.Addr().String()
	logger.Infof("Executor: %v", executorServer.Address)

	go executorServer.Heartbeat()

	grpcS := grpc.NewServer()
	pb.RegisterGueryExecutorServer(grpcS, executorServer)
	grpcS.Serve(listener)
}
