package executor

import (
	"context"
	"fmt"
	"github.com/gotodb/gotodb/logger"
	"github.com/gotodb/gotodb/pb"
	"github.com/gotodb/gotodb/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"strings"
)

func (e *Executor) SetupWriters(ctx context.Context, empty *pb.Empty) (*pb.Empty, error) {
	logger.Infof("SetupWriters start")
	var err error

	ip := strings.Split(e.Address, ":")[0]

	for i := 0; i < len(e.OutputLocations); i++ {
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
					listener.Close()
					return

				default:
					conn, err := listener.Accept()
					if err != nil {
						logger.Errorf("failed to accept: %v", err)
						continue
					}
					logger.Infof("connect %v", conn)

					go func(w io.Writer) {
						err := util.CopyBuffer(pr, w)
						if err != nil && err != io.EOF {
							logger.Errorf("failed to CopyBuffer: %v", err)
						}
						if wc, ok := w.(io.WriteCloser); ok {
							wc.Close()
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

	for i := 0; i < len(e.InputLocations); i++ {
		pr, pw := io.Pipe()
		e.Readers = append(e.Readers, pr)

		conn, err := grpc.Dial(e.InputLocations[i].GetURL(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Errorf("failed to connect to %v: %v", e.InputLocations[i], err)
			return empty, err
		}
		client := pb.NewGueryAgentClient(conn)
		inputChannelLocation, err := client.GetOutputChannelLocation(context.Background(), e.InputLocations[i])

		if err != nil {
			logger.Errorf("failed to connect %v: %v", e.InputLocations[i], err)
			return empty, err
		}

		conn.Close()

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
			pw.Close()
			if rc, ok := r.(io.ReadCloser); ok {
				rc.Close()
			}
		}(cconn)
	}

	logger.Infof("SetupReaders Input=%v, Output=%v", e.InputChannelLocations, e.OutputChannelLocations)
	return empty, err
}
