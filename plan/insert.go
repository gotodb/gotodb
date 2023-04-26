package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/plan/operator"
)

type InsertNode struct {
	Input    Node
	Output   Node
	Metadata *metadata.Metadata
	Catalog  string
	Schema   string
	Table    string
	Name     string
	Columns  []string
}

func NewInsertNode(runtime *config.Runtime, input Node, qualifiedName parser.IQualifiedNameContext, columns parser.IColumnAliasesContext) *InsertNode {

	name := qualifiedName.GetText()
	catalog, schema, table := metadata.SplitTableName(runtime, name)

	res := &InsertNode{
		Input:    input,
		Metadata: metadata.NewMetadata(),
		Catalog:  catalog,
		Schema:   schema,
		Table:    table,
		Name:     name,
	}

	if columns != nil {
		for _, context := range columns.AllIdentifier() {
			itemNode := operator.NewIdentifierNode(runtime, context)
			res.Columns = append(res.Columns, itemNode.GetText())
		}
	}

	return res
}

func (n *InsertNode) GetType() NodeType {
	return NodeTypeSelect
}

func (n *InsertNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *InsertNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *InsertNode) GetOutput() Node {
	return n.Output
}

func (n *InsertNode) SetOutput(output Node) {
	n.Output = output
}

func (n *InsertNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *InsertNode) SetMetadata() error {
	if err := n.Input.SetMetadata(); err != nil {
		return err
	}

	n.Metadata = metadata.NewMetadata()
	col := metadata.NewColumnMetadata(datatype.INT64, n.Catalog, n.Schema, n.Table, "affected")
	n.Metadata.AppendColumn(col)

	return nil
}

func (n *InsertNode) String() string {
	res := "InsertNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Name: " + n.Name + "\n"
	res += "Metadata:" + fmt.Sprintf("%v", n.Metadata) + "\n"
	res += "Columns:" + fmt.Sprintf("%v", n.Columns) + "\n"
	res += "}\n"
	return res
}
