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
	"google.golang.org/grpc"
	"log"
	"net"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	pb.UnimplementedWorkerServer
}

func (s *server) SendInstruction(ctx context.Context, instruction *pb.Instruction) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.New(instruction.Location.Name)
	_, err := exec.SendInstruction(ctx, instruction)
	if err != nil {
		return empty, err
	}
	return empty, err
}

func (s *server) SetupWriters(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.Get(loc.Name)
	_, err := exec.SetupWriters(ctx, nil)
	if err != nil {
		return empty, err
	}
	return empty, err
}

func (s *server) SetupReaders(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.Get(loc.Name)
	_, err := exec.SetupReaders(ctx, nil)
	if err != nil {
		return empty, err
	}
	return empty, err
}

func (s *server) GetOutputChannelLocation(ctx context.Context, loc *pb.Location) (*pb.Location, error) {
	exec := executor.Get(loc.Name)
	output, err := exec.GetOutputChannelLocation(ctx, loc)
	if err != nil {
		return &pb.Location{}, err
	}
	return output, err
}

func (s *server) Run(ctx context.Context, loc *pb.Location) (*pb.Empty, error) {
	empty := new(pb.Empty)
	exec := executor.Get(loc.Name)
	_, err := exec.Run(ctx, nil)
	if err != nil {
		return empty, err
	}
	return empty, err
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
