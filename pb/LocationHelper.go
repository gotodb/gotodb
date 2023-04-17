package pb

import (
	"fmt"
)

func (loc *Location) GetURL() string {
	return fmt.Sprintf("%v:%v", loc.Address, loc.Port)
}

func (loc *Location) GetRPC() string {
	return fmt.Sprintf("%v:%v", loc.Address, loc.RPCPort)
}

func (loc *Location) NewChannel(i int32) *Location {
	return &Location{
		Name:         loc.Name,
		Address:      loc.Address,
		Port:         loc.Port,
		RPCPort:      loc.RPCPort,
		ChannelIndex: i,
	}
}
