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
	logger.Infof("Instruction: %v", instruction.TaskType)
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
	return res, nil
}

func (e *Executor) Run(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	res := &pb.Empty{}
	nodeType := stage.JobType(e.Instruction.TaskType)

	switch nodeType {
	case stage.JobTypeScan:
		go e.RunScan()
	case stage.JobTypeSelect:
		go e.RunSelect()
	case stage.JobTypeGroupBy:
		go e.RunGroupBy()
	case stage.JobTypeJoin:
		go e.RunJoin()
	case stage.JobTypeHashJoin:
		go e.RunHashJoin()
	case stage.JobTypeShuffle:
		go e.RunShuffle()
	case stage.JobTypeDuplicate:
		go e.RunDuplicate()
	case stage.JobTypeAggregate:
		go e.RunAggregate()
	case stage.JobTypeAggregateFuncGlobal:
		go e.RunAggregateFuncGlobal()
	case stage.JobTypeAggregateFuncLocal:
		go e.RunAggregateFuncLocal()
	case stage.JobTypeLimit:
		go e.RunLimit()
	case stage.JobTypeFilter:
		go e.RunFilter()
	case stage.JobTypeOrderByLocal:
		go e.RunOrderByLocal()
	case stage.JobTypeOrderBy:
		go e.RunOrderBy()
	case stage.JobTypeUnion:
		go e.RunUnion()
	case stage.JobTypeShow:
		go e.RunShow()
	case stage.JobTypeBalance:
		go e.RunBalance()
	case stage.JobTypeDistinctLocal:
		go e.RunDistinctLocal()
	case stage.JobTypeDistinctGlobal:
		go e.RunDistinctGlobal()
	default:
		return res, fmt.Errorf("unknown job type")
	}
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
