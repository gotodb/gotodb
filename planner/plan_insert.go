package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
)

type InsertPlan struct {
	Input    Plan
	Output   Plan
	Metadata *metadata.Metadata
	Catalog  string
	Schema   string
	Table    string
	Name     string
	Columns  []string
}

func NewInsertPlan(runtime *config.Runtime, input Plan, qualifiedName parser.IQualifiedNameContext, columns parser.IColumnAliasesContext) *InsertPlan {

	name := qualifiedName.GetText()
	catalog, schema, table := metadata.SplitTableName(runtime, name)

	res := &InsertPlan{
		Input:    input,
		Metadata: metadata.NewMetadata(),
		Catalog:  catalog,
		Schema:   schema,
		Table:    table,
		Name:     name,
	}

	if columns != nil {
		for _, context := range columns.AllIdentifier() {
			itemNode := NewIdentifierNode(runtime, context)
			res.Columns = append(res.Columns, itemNode.GetText())
		}
	}

	return res
}

func (n *InsertPlan) GetType() NodeType {
	return NodeTypeSelect
}

func (n *InsertPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *InsertPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *InsertPlan) GetOutput() Plan {
	return n.Output
}

func (n *InsertPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *InsertPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *InsertPlan) SetMetadata() error {
	if err := n.Input.SetMetadata(); err != nil {
		return err
	}

	n.Metadata = metadata.NewMetadata()
	col := metadata.NewColumnMetadata(datatype.INT64, n.Catalog, n.Schema, n.Table, "affected")
	n.Metadata.AppendColumn(col)

	return nil
}

func (n *InsertPlan) String() string {
	res := "InsertPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Name: " + n.Name + "\n"
	res += "Metadata:" + fmt.Sprintf("%v", n.Metadata) + "\n"
	res += "Columns:" + fmt.Sprintf("%v", n.Columns) + "\n"
	res += "}\n"
	return res
}
