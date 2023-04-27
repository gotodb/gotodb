package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/metadata"
)

type ScanPlan struct {
	Catalog   string
	Schema    string
	Table     string
	Name      string
	Metadata  *metadata.Metadata
	Connector connector.Connector
	Output    Plan
	Filters   []*BooleanExpressionNode
}

func NewScanPlan(runtime *config.Runtime, name string) *ScanPlan {
	catalog, schema, table := metadata.SplitTableName(runtime, name)
	res := &ScanPlan{
		Catalog: catalog,
		Schema:  schema,
		Table:   table,
		Name:    name,
	}
	return res
}

func (n *ScanPlan) GetType() NodeType {
	return NodeTypeScan
}

func (n *ScanPlan) String() string {
	res := "ScanPlan {\n"
	res += "Name: " + n.Name + "\n"
	res += "Metadata:" + fmt.Sprintf("%v", n.Metadata) + "\n"
	res += "Filters:" + fmt.Sprintf("%v", n.Filters) + "\n"
	res += "}\n"
	return res
}

func (n *ScanPlan) GetInputs() []Plan {
	return []Plan{}
}

func (n *ScanPlan) SetInputs(_ []Plan) {
}

func (n *ScanPlan) GetOutput() Plan {
	return n.Output
}

func (n *ScanPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *ScanPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *ScanPlan) SetMetadata() error {
	if n.Metadata != nil {
		return nil
	}
	ctr, err := connector.NewConnector(n.Catalog, n.Schema, n.Table)
	if err != nil {
		return err
	}

	md, err := ctr.GetMetadata()
	if err != nil {
		return err
	}
	n.Connector = ctr
	n.Metadata = md.Copy()
	n.Metadata.Reset()

	return err
}
