package executor

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/pb"
	"google.golang.org/grpc"
)

func (e *Executor) Heartbeat() {
	for {
		if err := e.DoHeartbeat(); err != nil {
			time.Sleep(3 * time.Second)
		}
	}
}

func (e *Executor) DoHeartbeat() error {
	grpcConn, err := grpc.Dial(e.AgentAddress, grpc.WithInsecure())
	if err != nil {
		logger.Errorf("DoHeartBeat failed: %v", err)
		return err
	}
	defer grpcConn.Close()

	client := pb.NewGueryAgentClient(grpcConn)
	stream, err := client.SendHeartbeat(context.Background())
	if err != nil {
		return err
	}

	ticker := time.NewTicker(1 * time.Second)
	quickTicker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-quickTicker.C:
			if e.IsStatusChanged {
				e.IsStatusChanged = false
				if err := e.SendOneHeartbeat(stream); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := e.SendOneHeartbeat(stream); err != nil {
				return err
			}
		}
	}
}

func (e *Executor) SendOneHeartbeat(stream pb.GueryAgent_SendHeartbeatClient) error {
	address, ports, err := net.SplitHostPort(e.Address)
	if err != nil {
		return err
	}
	var port int32
	fmt.Sscanf(ports, "%d", &port)

	hb := &pb.ExecutorHeartbeat{
		Location: &pb.Location{
			Name:    e.Name,
			Address: address,
			Port:    port,
		},
		Status: e.Status,
		Infos:  e.Infos,
	}

	if e.Instruction != nil {
		hb.TaskId = e.Instruction.TaskId
	}

	if err := stream.Send(hb); err != nil {
		logger.Errorf("failed to SendOneHeartbeat: %v, %v", err, e.AgentAddress)
		return err
	}
	return nil
}
