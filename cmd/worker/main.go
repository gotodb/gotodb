/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a server for Greeter service.
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
	"time"
)

var (
	port    = flag.Int("port", 50051, "The rpc server port")
	tcpPort = flag.Int("tcp port", 50052, "The tcp server port")
)

var hostname string

var address = "127.0.0.1"

var etcdCfg = clientv3.Config{
	Endpoints: []string{
		"http://localhost:2379",
	},
	DialTimeout:          time.Second * 30,
	DialKeepAliveTimeout: time.Second * 30,
}

type server struct {
	pb.UnimplementedWorkerServer
}

func (s *server) SendInstruction(ctx context.Context, instruction *pb.Instruction) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.New(instruction.Location.Name)
	_, err := exec.SendInstruction(ctx, instruction)
	if err != nil {
		executor.Delete(instruction.Location.Name)
		return empty, err
	}
	return empty, err
}

func (s *server) SetupWriters(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.Get(loc.Name)
	_, err := exec.SetupWriters(ctx, nil)
	if err != nil {
		executor.Delete(loc.Name)
		return empty, err
	}
	return empty, err
}

func (s *server) SetupReaders(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.Get(loc.Name)
	_, err := exec.SetupReaders(ctx, nil)
	if err != nil {
		executor.Delete(loc.Name)
		return empty, err
	}
	return empty, err
}

func (s *server) Run(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.Get(loc.Name)
	defer executor.Delete(loc.Name)
	_, err := exec.Run(ctx, nil)
	if err != nil {
		return empty, err
	}
	return empty, err
}

func assign(conn net.Conn) {
	//创建消息缓冲区
	buffer := make([]byte, 10240)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			_ = fmt.Errorf("%v", err)
			_ = conn.Close()
			return
		}
		var loc *pb.Location
		if err := msgpack.Unmarshal(buffer[0:n], &loc); err != nil {
			fmt.Printf("%v", err)
			_ = conn.Close()
			return
		}
		fmt.Printf("接收到长度:%d, 内容:%v", n, loc)

		exec := executor.Get(loc.Name)
		exec.OutputConnChan[loc.ChannelIndex] <- conn
	}
}

func main() {
	flag.Parse()
	go func() {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *tcpPort))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		for {
			//循环接入所有客户端得到专线连接
			conn, err := listener.Accept()
			if err != nil {
				log.Fatalf("failed to accept: %v", err)
			}
			fmt.Printf("来自连接: %s", conn.RemoteAddr())
			//开辟独立协程与该客聊天
			go assign(conn)
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// 服务注册
	go func() {
		ServiceRegistry(*port, *tcpPort)
	}()
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func ServiceRegistry(rpcPort int, tcpPort int) {
	hostname, _ = os.Hostname()
	cli, err := clientv3.New(etcdCfg)
	if err != nil {
		panic(err)
	}
	key := fmt.Sprintf("%s/%s-%d", "worker", hostname, os.Getpid())
	endpoint := fmt.Sprintf("%s:%d:%d", address, rpcPort, tcpPort)
	ctx := context.Background()
	// 过期时间: 3秒钟
	// 创建租约
	lease, err := cli.Grant(ctx, 3)
	if err != nil {
		panic(err)
	}
	// put kv
	_, err = cli.Put(ctx, key, endpoint, clientv3.WithLease(lease.ID))
	if err != nil {
		panic(err)
	}
	// 保持租约不过期
	klRes, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		panic(err)
	}
	// 监听续约情况
	for range klRes {
	}
	fmt.Println("stop keeping lease alive")
}
