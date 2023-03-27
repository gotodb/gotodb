package executor

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/pb"
	"github.com/vmihailenco/msgpack"
)

func (e *Executor) SetRuntime(instruction *pb.Instruction) (err error) {
	var runtime config.Runtime
	if err = msgpack.Unmarshal(instruction.RuntimeBytes, &runtime); err != nil {
		return err
	}
	config.Conf.Runtime = &runtime
	return nil
}
