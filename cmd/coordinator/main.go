package main

import (
	"context"
	"encoding/json"
	"flag"
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
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	configFile = flag.String("c", "config.yaml", "The configure file")
)

type WorkerNodes map[string]*pb.Location

var workerNode = make(WorkerNodes)

func (n WorkerNodes) GetExecutorLoc() *pb.Location {
	if len(workerNode) == 0 {
		return nil
	}
	var keys []string
	for v := range workerNode {
		keys = append(keys, v)
	}
	randomKey := keys[rand.Intn(len(keys))]

	return &pb.Location{
		Name:    "executor_" + uuid.NewV4().String(),
		Address: workerNode[randomKey].Address,
		Port:    workerNode[randomKey].Port,
		RPCPort: workerNode[randomKey].RPCPort,
	}
}

func (n WorkerNodes) HasExecutor() bool {
	if len(workerNode) == 0 {
		return false
	} else {
		return true
	}
}

func Query(w http.ResponseWriter, req *http.Request) {
	sqlStr := "/*+partition_number=4*/select * from http.toutiao.info where _http = '{ \"url\": \"http://127.0.0.1:2379/v2/keys/queue?recursive=true&sorted=true\", \"dataPath\": \"node.nodes\", \"timeout\": 2000 }'"

	var query struct {
		SQL string `json:"sql"`
	}
	err := json.NewDecoder(req.Body).Decode(&query)
	if err == nil && query.SQL != "" {
		sqlStr = query.SQL
	}

	//sqlStr := "show COLUMNS from test.test.csv"
	hint := optimizer.ParseHint(sqlStr)
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

	runtime := config.NewRuntime()
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

	partitionNumber := config.Conf.Runtime.ParallelNumber
	if hint.PartitionNumber > 0 {
		partitionNumber = hint.PartitionNumber
	}
	stageJobs, err := stage.CreateJob(logicalTree, workerNode, partitionNumber)
	if err != nil {
		panic(err)
		return
	}
	var (
		buf        []byte
		runtimeBuf []byte
		taskId     = fmt.Sprintf("%v_%v", time.Now().Format("20060102150405"), uuid.NewV4().String())
		aggJob     stage.Job
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

		url := job.GetLocation().GetRPC()
		if _, ok := grpcConn[url]; !ok {
			_grpc, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				logger.Errorf("failed to dial: %v", err)
				break
			}
			grpcConn[url] = pb.NewWorkerClient(_grpc)
		}
		aggJob = job
	}
	for _, instruction := range instructions {
		client := grpcConn[instruction.GetLocation().GetRPC()]
		if _, err = client.SendInstruction(context.Background(), instruction); err != nil {
			logger.Errorf("failed to SendInstruction: %v", err)
			break
		}
	}
	var wg sync.WaitGroup
	for _, instruction := range instructions {
		client := grpcConn[instruction.GetLocation().GetRPC()]
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

	conn, err := net.Dial("tcp", aggLoc.GetURL())
	if err != nil {
		logger.Errorf("failed to connect to input channel %v: %v", aggLoc, err)
		return
	}
	bytes, _ := msgpack.Marshal(aggLoc)
	conn.Write(bytes)

	if err = util.ReadObject(conn, md); err != nil {
		logger.Errorf("read md err: %v", err)
		return
	}

	if msg, err = json.MarshalIndent(md, "", "    "); err != nil {
		logger.Errorf("json marshal: %v", err)
		return
	}

	msg = append(msg, []byte("\n")...)

	w.Write(msg)

	rbReader := row.NewRowsBuffer(md, conn, nil)

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

		w.Write(msg)
	}
	wg.Wait()
}

func main() {
	flag.Parse()
	if err := config.Load(*configFile); err != nil {
		log.Fatal(err)
	}
	fmt.Println("start gotodb coordinator")
	workerDiscovery()
	http.HandleFunc("/query", Query)
	if err := http.ListenAndServe(strconv.Itoa(config.Conf.Coordinator.HttpPort), nil); err != nil {
		log.Fatal(err)
	}
}

var serviceLocker = sync.Mutex{}

func workerDiscovery() {
	cli, err := clientv3.New(config.NewEtcd())
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	// 获取当前所有服务入口
	getRes, err := cli.Get(ctx, "worker", clientv3.WithPrefix())
	if err != nil {
		logger.Errorf("read line: %v", err)
		return
	}
	serviceLocker.Lock()
	for _, v := range getRes.Kvs {
		key := string(v.Key)
		s := strings.Split(string(v.Value), ":")
		println(s)
		rpcPort, _ := strconv.Atoi(s[1])
		tcpPort, _ := strconv.Atoi(s[2])
		workerNode[key] = &pb.Location{
			Name:    key,
			Address: s[0],
			Port:    int32(tcpPort),
			RPCPort: int32(rpcPort),
		}
	}
	serviceLocker.Unlock()
	go func() {
		ch := cli.Watch(ctx, "worker", clientv3.WithPrefix(), clientv3.WithPrevKV())
		for v := range ch {
			for _, v := range v.Events {
				key := string(v.Kv.Key)
				s := string(v.Kv.Value)
				preEndpoint := ""
				if v.PrevKv != nil {
					preEndpoint = string(v.PrevKv.Value)
				}
				switch v.Type {
				// PUT
				case 0:
					serviceLocker.Lock()
					s := strings.Split(s, ":")
					rpcPort, _ := strconv.Atoi(s[1])
					tcpPort, _ := strconv.Atoi(s[2])
					workerNode[key] = &pb.Location{
						Name:    key,
						Address: s[0],
						Port:    int32(tcpPort),
						RPCPort: int32(rpcPort),
					}
					serviceLocker.Unlock()
					fmt.Printf(
						"[service_endpoint_change] put endpoint, key: %s, endpoint: %s\n",
						key, s,
					)
				// DELETE
				case 1:
					serviceLocker.Lock()
					delete(workerNode, key)
					serviceLocker.Unlock()
					fmt.Printf(
						"[service_endpoint_change] delete endpoint, key: %s, endpoint: %s\n",
						key, preEndpoint,
					)
				}
			}
		}
	}()
}
