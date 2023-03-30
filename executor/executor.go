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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"strings"
	"sync"
)

type Executor struct {
	sync.Mutex
	Name string

	Instruction                                   *pb.Instruction
	StageJob                                      stage.Job
	InputChannelLocations, OutputChannelLocations []*pb.Location
	Readers                                       []io.Reader
	Writers                                       []io.Writer

	Status          pb.TaskStatus
	IsStatusChanged bool
	Infos           []*pb.LogInfo

	DoneChan chan int
}

var executors = make(map[string]*Executor)

func New(name string) *Executor {
	res := &Executor{
		Name:     name,
		DoneChan: make(chan int),
		Infos:    []*pb.LogInfo{},
		Status:   pb.TaskStatus_TODO,
	}
	executors[name] = res
	return res
}

func Get(name string) *Executor {
	return executors[name]
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

	select {
	case <-e.DoneChan:
	default:
		close(e.DoneChan)
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

func (e *Executor) SetupWriters(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	logger.Infof("SetupWriters start")
	var err error

	ip := strings.Split(e.Instruction.Location.Address, ":")[0]

	for range e.StageJob.GetOutputs() {
		pr, pw := io.Pipe()
		e.Writers = append(e.Writers, pw)
		listener, err := net.Listen("tcp", ip+":0")
		if err != nil {
			logger.Errorf("failed to open listener: %v", err)
			return nil, fmt.Errorf("failed to open listener: %v", err)
		}
		e.OutputChannelLocations = append(e.OutputChannelLocations,
			&pb.Location{
				Name:    e.Name,
				Address: util.GetHostFromAddress(listener.Addr().String()),
				Port:    util.GetPortFromAddress(listener.Addr().String()),
			},
		)

		go func() {
			for {
				select {
				case <-e.DoneChan:
					_ = listener.Close()
					return

				default:
					conn, err := listener.Accept()
					if err != nil {
						logger.Errorf("failed to accept: %v", err)
						continue
					}
					logger.Infof("connect %v", conn.RemoteAddr())

					go func(w io.Writer) {
						err := util.CopyBuffer(pr, w)
						if err != nil && err != io.EOF {
							logger.Errorf("failed to CopyBuffer: %v", err)
						}
						if wc, ok := w.(io.WriteCloser); ok {
							_ = wc.Close()
						}
					}(conn)
				}
			}
		}()
	}

	logger.Infof("SetupWriters Input=%v, Output=%v", e.InputChannelLocations, e.OutputChannelLocations)
	return empty, err
}

func (e *Executor) SetupReaders(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	var err error
	logger.Infof("SetupReaders start")
	for _, location := range e.StageJob.GetInputs() {
		pr, pw := io.Pipe()
		e.Readers = append(e.Readers, pr)

		conn, err := grpc.Dial(location.GetURL(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Errorf("failed to connect to %v: %v", location, err)
			return empty, err
		}
		client := pb.NewWorkerClient(conn)
		inputChannelLocation, err := client.GetOutputChannelLocation(context.Background(), location)

		if err != nil {
			logger.Errorf("failed to connect %v: %v", location, err)
			return empty, err
		}

		_ = conn.Close()

		e.InputChannelLocations = append(e.InputChannelLocations, inputChannelLocation)
		cconn, err := net.Dial("tcp", inputChannelLocation.GetURL())
		if err != nil {
			logger.Errorf("failed to connect to input channel %v: %v", inputChannelLocation, err)
			return empty, err
		}
		logger.Infof("connect to %v", inputChannelLocation)

		go func(r io.Reader) {
			err := util.CopyBuffer(r, pw)
			if err != nil && err != io.EOF {
				logger.Errorf("failed to CopyBuffer: %v", err)
			}
			_ = pw.Close()
			if rc, ok := r.(io.ReadCloser); ok {
				_ = rc.Close()
			}
		}(cconn)
	}

	logger.Infof("SetupReaders Input=%v, Output=%v", e.InputChannelLocations, e.OutputChannelLocations)
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

func (e *Executor) GetOutputChannelLocation(ctx context.Context, location *pb.Location) (*pb.Location, error) {
	if int(location.ChannelIndex) >= len(e.OutputChannelLocations) {
		return nil, fmt.Errorf("ChannelLocation %v not found: %v", location.ChannelIndex, location)
	}
	return e.OutputChannelLocations[location.ChannelIndex], nil
}
