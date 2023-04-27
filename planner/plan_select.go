package planner

import (
	"fmt"
	"github.com/gotodb/gotodb/pkg/parser"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
)

type SelectPlan struct {
	Input         Plan
	Output        Plan
	Metadata      *metadata.Metadata
	SetQuantifier *datatype.QuantifierType
	SelectItems   []*SelectItemNode
	Having        *BooleanExpressionNode
	IsAggregate   bool
}

func NewSelectPlan(runtime *config.Runtime, input Plan, sq parser.ISetQuantifierContext, items []parser.ISelectItemContext, having parser.IBooleanExpressionContext) *SelectPlan {
	res := &SelectPlan{
		Input:         input,
		Metadata:      metadata.NewMetadata(),
		SetQuantifier: nil,
		SelectItems:   []*SelectItemNode{},
		Having:        nil,
	}
	if sq != nil {
		q := datatype.ToQuantifierType(sq.GetText())
		res.SetQuantifier = &q
	}
	for i := 0; i < len(items); i++ {
		itemNode := NewSelectItemNode(runtime, items[i])
		res.SelectItems = append(res.SelectItems, itemNode)
		if itemNode.IsAggregate() {
			res.IsAggregate = true
		}
	}

	if having != nil {
		res.Having = NewBooleanExpressionNode(runtime, having)
		if res.Having.IsAggregate() {
			res.IsAggregate = true
		}
	}
	return res
}

func (n *SelectPlan) GetType() NodeType {
	return NodeTypeSelect
}

func (n *SelectPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *SelectPlan) SetInputs(inputs []Plan) {
	n.Input = inputs[0]
}

func (n *SelectPlan) GetOutput() Plan {
	return n.Output
}

func (n *SelectPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *SelectPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *SelectPlan) SetMetadata() error {
	if err := n.Input.SetMetadata(); err != nil {
		return err
	}
	md := n.Input.GetMetadata()
	var colNames []string
	var colTypes []datatype.Type
	for _, item := range n.SelectItems {
		names, types, err := item.GetNamesAndTypes(md)
		if err != nil {
			return err
		}
		colNames = append(colNames, names...)
		colTypes = append(colTypes, types...)
	}

	if len(colNames) != len(colTypes) {
		return fmt.Errorf("length error")
	}
	n.Metadata = metadata.NewMetadata()
	for i, name := range colNames {
		t := colTypes[i]
		column := metadata.NewColumnMetadata(t, strings.Split(name, ".")...)
		n.Metadata.AppendColumn(column)
	}
	n.Metadata.Reset()

	return nil
}

func (n *SelectPlan) String() string {
	res := "SelectPlan {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Metadata: " + fmt.Sprint(n.Metadata) + "\n"
	res += "SelectItems: " + fmt.Sprint(n.SelectItems) + "\n"
	res += "}\n"
	return res
}
