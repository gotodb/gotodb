package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/filesystem/partition"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/plan/operator"
)

type ScanNode struct {
	Catalog       string
	Schema        string
	Table         string
	Name          string
	Metadata      *metadata.Metadata
	InputMetadata *metadata.Metadata
	PartitionInfo *partition.Info
	Output        Node
	Filters       []*operator.BooleanExpressionNode
}

func NewScanNode(runtime *config.Runtime, name string) *ScanNode {
	catalog, schema, table := metadata.SplitTableName(runtime, name)
	res := &ScanNode{
		Catalog: catalog,
		Schema:  schema,
		Table:   table,
		Name:    name,
	}
	return res
}

func (n *ScanNode) GetType() NodeType {
	return NodeTypeScan
}

func (n *ScanNode) String() string {
	res := "ScanNode {\n"
	res += "Name: " + n.Name + "\n"
	res += "Metadata:" + fmt.Sprintf("%v", n.Metadata) + "\n"
	res += "Filters:" + fmt.Sprintf("%v", n.Filters) + "\n"
	res += "}\n"
	return res
}

func (n *ScanNode) GetInputs() []Node {
	return []Node{}
}

func (n *ScanNode) SetInputs(_ []Node) {
}

func (n *ScanNode) GetOutput() Node {
	return n.Output
}

func (n *ScanNode) SetOutput(output Node) {
	n.Output = output
}

func (n *ScanNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *ScanNode) SetMetadata() error {
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
	n.Metadata = md.Copy()
	n.Metadata.Reset()
	n.InputMetadata = md.Copy()
	n.InputMetadata.Reset()

	n.PartitionInfo, err = ctr.GetPartitionInfo()

	return err
}
