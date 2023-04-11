package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/executor"
	"github.com/gotodb/gotodb/pb"
	"github.com/vmihailenco/msgpack"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var (
	configFile = flag.String("c", "config.yaml", "The configure file")
)

var hostname string

type worker struct {
	pb.UnimplementedWorkerServer
}

func (s *worker) SendInstruction(ctx context.Context, instruction *pb.Instruction) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.New(instruction.Location.Name)
	err := exec.SendInstruction(ctx, instruction)
	if err != nil {
		executor.Delete(instruction.Location.Name)
		return empty, err
	}

	return empty, err
}

func (s *worker) Run(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec, err := executor.Get(loc.Name)
	if err != nil {
		return empty, err
	}
	defer executor.Delete(loc.Name)

	if _, err := exec.Run(ctx, empty); err != nil {
		return empty, err
	}
	return empty, err
}

func dispatch(conn net.Conn) {
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Printf("fail to read message: %v\n", err)
		_ = conn.Close()
		return
	}
	var loc *pb.Location
	if err := msgpack.Unmarshal(buffer[0:n], &loc); err != nil {
		fmt.Printf("fail to unmarshal message: %v\n", err)
		_ = conn.Close()
		return
	}
	fmt.Printf("接收到长度:%d, 内容:%v", n, loc)

	exec, err := executor.Get(loc.Name)
	if err != nil {
		fmt.Printf("fail to get executor: %v\n", err)
		_ = conn.Close()
		return
	}

	exec.Writers[loc.ChannelIndex] = conn
}

func main() {
	flag.Parse()
	if err := config.Load(*configFile); err != nil {
		log.Fatal(err)
	}
	fmt.Println("start gotodb worker")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Conf.Worker.TCPPort))
		if err != nil {
			log.Fatalf("failed to listen tcp: %v", err)
			return
		}
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Printf("failed to accept: %v\n", err)
				continue
			}

			if err := conn.SetDeadline(time.Now().Add(5 * time.Minute)); err != nil {
				fmt.Printf("connection dead line: %v", err)
				return
			}

			go dispatch(conn)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.Conf.Worker.RPCPort))
		if err != nil {
			log.Fatalf("failed to listen rpc: %v", err)
		}
		// 服务注册

		s := grpc.NewServer()
		pb.RegisterWorkerServer(s, &worker{})
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		workerRegistry()
	}()

	wg.Wait()
	fmt.Println("stop gotodb worker")
}

func workerRegistry() {
	hostname, _ = os.Hostname()
	cli, err := clientv3.New(config.NewEtcd())
	if err != nil {
		log.Fatalf("failed to new etcd client: %v", err)
	}

	ctx := context.Background()
	lease, err := cli.Grant(ctx, 3)
	if err != nil {
		log.Fatalf("failed to grant lease: %v", err)
	}

	key := fmt.Sprintf("%s/%s-%d", "worker", hostname, os.Getpid())
	endpoint := fmt.Sprintf("%s:%d:%d", config.Conf.Worker.IP, config.Conf.Worker.RPCPort, config.Conf.Worker.TCPPort)
	_, err = cli.Put(ctx, key, endpoint, clientv3.WithLease(lease.ID))
	if err != nil {
		log.Fatalf("failed to regiseter worker: %v", err)
	}

	klRes, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		log.Fatalf("failed to keep alive with etcd: %v", err)
	}

	for range klRes {
	}
}
