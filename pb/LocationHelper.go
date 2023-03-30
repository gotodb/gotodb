package pb

import (
	"fmt"
)

func (loc *Location) GetURL() string {
	return fmt.Sprintf("%v:%v", loc.Address, loc.Port)
}
