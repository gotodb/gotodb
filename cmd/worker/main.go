package main

import (
	"context"
	"flag"
	"fmt"
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
	ip           = flag.String("ip", "127.0.0.1", "The worker ip")
	tcpPort      = flag.Int("tcp-port", 50051, "The worker tcp port")
	rpcPort      = flag.Int("rpc-port", 50052, "The worker rpc port")
	etcdEndpoint = flag.String("etcd-endpoint", "http://127.0.0.1:2379", "The etcd endpoint")
)

var hostname string

var etcdCfg = clientv3.Config{
	Endpoints:            []string{},
	DialTimeout:          time.Second * 5,
	DialKeepAliveTimeout: time.Second * 5,
}

type worker struct {
	pb.UnimplementedWorkerServer
}

func (s *worker) SendInstruction(ctx context.Context, instruction *pb.Instruction) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.New(instruction.Location.Name)
	_, err := exec.SendInstruction(ctx, instruction)
	if err != nil {
		executor.Delete(instruction.Location.Name)
		return empty, err
	}
	return empty, err
}

func (s *worker) SetupWriters(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec, err := executor.Get(loc.Name)
	if err != nil {
		return empty, err
	}

	if _, err := exec.SetupWriters(ctx, nil); err != nil {
		executor.Delete(loc.Name)
		return empty, err
	}
	return empty, err
}

func (s *worker) SetupReaders(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec, err := executor.Get(loc.Name)
	if err != nil {
		return empty, err
	}

	if _, err := exec.SetupReaders(ctx, nil); err != nil {
		executor.Delete(loc.Name)
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

	if _, err := exec.Run(ctx, nil); err != nil {
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

	exec.OutputConnChan[loc.ChannelIndex] <- conn
}

func main() {
	flag.Parse()
	fmt.Println("start gotodb worker")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *tcpPort))
		if err != nil {
			log.Fatalf("failed to listen tcp: %v", err)
		}
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Printf("failed to accept: %v\n", err)
				continue
			}
			go dispatch(conn)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *rpcPort))
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
		workerRegistry(*rpcPort, *tcpPort)
	}()

	wg.Wait()
	fmt.Println("stop gotodb worker")
}

func workerRegistry(rpcPort int, tcpPort int) {
	hostname, _ = os.Hostname()
	etcdCfg.Endpoints = []string{*etcdEndpoint}
	cli, err := clientv3.New(etcdCfg)
	if err != nil {
		log.Fatalf("failed to new etcd client: %v", err)
	}

	ctx := context.Background()
	lease, err := cli.Grant(ctx, 3)
	if err != nil {
		log.Fatalf("failed to grant lease: %v", err)
	}

	key := fmt.Sprintf("%s/%s-%d", "worker", hostname, os.Getpid())
	endpoint := fmt.Sprintf("%s:%d:%d", *ip, rpcPort, tcpPort)
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
