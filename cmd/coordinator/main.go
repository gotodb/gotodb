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
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/optimizer"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/plan"
	"github.com/gotodb/gotodb/plan/operator"
	"github.com/gotodb/gotodb/row"
	"github.com/gotodb/gotodb/stage"
	"github.com/gotodb/gotodb/util"
	uuid "github.com/satori/go.uuid"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

func main() {
	sqlStr := "select sum(a.var1), a.var2, a.data_source from test.test.csv as a limit 10"
	//sqlStr := "show COLUMNS from test.test.csv"
	inputStream := antlr.NewInputStream(sqlStr)
	lexer := parser.NewSqlLexer(parser.NewCaseChangingStream(inputStream, true))
	p := parser.NewSqlParser(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
	errListener := parser.NewErrorListener()
	p.AddErrorListener(errListener)
	tree := p.SingleStatement()
	if errListener.HasError() {
		panic(errListener)
		return
	}

	runtime := config.NewConfigRuntime()
	logicalTree := plan.NewNodeFromSingleStatement(runtime, tree)

	//SetMetaData
	if err := logicalTree.SetMetadata(); err != nil {
		panic(err)
		return
	}

	if err := optimizer.DeleteRenameNode(logicalTree); err != nil {
		panic(err)
		return
	}

	if err := optimizer.FilterColumns(logicalTree, []string{}); err != nil {
		panic(err)
		return
	}

	if err := optimizer.PredicatePushDown(logicalTree, []*operator.BooleanExpressionNode{}); err != nil {
		panic(err)
		return
	}

	if err := optimizer.ExtractAggFunc(logicalTree); err != nil {
		panic(err)
		return
	}

	executorHeap := util.NewHeap()
	heap.Init(executorHeap)
	heap.Push(executorHeap, util.NewItem(&pb.Location{Name: "localhost", Address: "localhost", Port: 50051}, 1))

	var stageJobs []stage.Job

	aggJob, err := stage.CreateJob(logicalTree, &stageJobs, executorHeap, 1)
	if err != nil {
		panic(err)
		return
	}
	var (
		buf        []byte
		runtimeBuf []byte
		taskId     = fmt.Sprintf("%v_%v", time.Now().Format("20060102150405"), uuid.NewV4().String())
	)

	var instructions []*pb.Instruction
	var grpcConn = make(map[string]pb.WorkerClient)
	for _, job := range stageJobs {
		if buf, err = msgpack.Marshal(job); err != nil {
			break
		}

		if runtimeBuf, err = msgpack.Marshal(runtime); err != nil {
			break
		}
		instructions = append(instructions, &pb.Instruction{
			TaskID:               taskId,
			TaskType:             int32(job.GetType()),
			EncodedStageJobBytes: buf,
			RuntimeBytes:         runtimeBuf,
			Location:             job.GetLocation(),
		})
		if _, ok := grpcConn[job.GetLocation().GetURL()]; !ok {
			_grpc, err := grpc.Dial(job.GetLocation().GetURL(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				logger.Errorf("failed to dial: %v", err)
				break
			}
			grpcConn[job.GetLocation().GetURL()] = pb.NewWorkerClient(_grpc)
		}

	}
	for _, instruction := range instructions {
		client := grpcConn[instruction.GetLocation().GetURL()]
		if _, err = client.SendInstruction(context.Background(), instruction); err != nil {
			logger.Errorf("failed to SendInstruction: %v", err)
			break
		}
		if _, err = client.SetupWriters(context.Background(), instruction.GetLocation()); err != nil {
			logger.Errorf("failed to SetupWriters: %v", err)
			break
		}
	}
	var wg sync.WaitGroup
	for _, instruction := range instructions {
		client := grpcConn[instruction.GetLocation().GetURL()]
		if _, err = client.SetupReaders(context.Background(), instruction.GetLocation()); err != nil {
			logger.Errorf("failed to SetupReaders: %v", err)
			break
		}
		wg.Add(1)
		go func(instruction *pb.Instruction) {
			defer wg.Done()
			if _, err = client.Run(context.Background(), instruction.GetLocation()); err != nil {
				logger.Errorf("failed to Run: %v", err)
			}
		}(instruction)
	}
	var (
		msg []byte
		r   *row.Row
	)
	md := &metadata.Metadata{}
	aggLoc := aggJob.GetLocation()
	conn, err := grpc.Dial(aggLoc.GetURL(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	client := pb.NewWorkerClient(conn)
	inputChannelLocation, err := client.GetOutputChannelLocation(context.Background(), aggLoc)
	if err != nil {
		logger.Errorf("failed to GetOutputChannelLocation: %v", err)
		return
	}
	conn.Close()

	cconn, err := net.Dial("tcp", inputChannelLocation.GetURL())
	if err != nil {
		logger.Errorf("failed to connect to input channel %v: %v", inputChannelLocation, err)
		return
	}

	if err = util.ReadObject(cconn, md); err != nil {
		logger.Errorf("read md err: %v", err)
		return
	}
	if msg, err = json.MarshalIndent(md, "", "    "); err != nil {
		logger.Errorf("json marshal: %v", err)
		return
	}
	msg = append(msg, []byte("\n")...)

	fmt.Println(string(msg))

	rbReader := row.NewRowsBuffer(md, cconn, nil)

	for {
		r, err = rbReader.ReadRow()

		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			logger.Errorf("read line: %v", err)
			return
		}

		var res []string
		for i := 0; i < len(r.Vals); i++ {
			res = append(res, fmt.Sprintf("%v", r.Vals[i]))
		}
		msg = []byte(strings.Join(res, ","))
		msg = append(msg, []byte("\n")...)

		fmt.Println(string(msg))
	}
	wg.Wait()

	// In order to switch coroutines
	time.Sleep(30 * time.Second)
}
