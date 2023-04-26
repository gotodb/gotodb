package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
)

type UsePlan struct {
	Catalog, Schema string
}

func NewUsePlan(_ *config.Runtime, ct, sh string) *UsePlan {
	return &UsePlan{
		Catalog: ct,
		Schema:  sh,
	}
}

func (n *UsePlan) GetType() NodeType {
	return NodeTypeUse
}

func (n *UsePlan) SetMetadata() error {
	return nil
}

func (n *UsePlan) GetMetadata() *metadata.Metadata {
	return nil
}

func (n *UsePlan) GetOutput() Plan {
	return nil
}

func (n *UsePlan) SetOutput(_ Plan) {
	return
}

func (n *UsePlan) GetInputs() []Plan {
	return nil
}

func (n *UsePlan) SetInputs(_ []Plan) {
	return
}

func (n *UsePlan) String() string {
	res := "UsePlan  {\n"
	res += n.Catalog + "." + n.Schema
	res += "}\n"
	return res
}
