package plan

import (
	"fmt"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/plan/operator"
	"strings"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
)

type SelectNode struct {
	Input         Node
	Output        Node
	Metadata      *metadata.Metadata
	SetQuantifier *datatype.QuantifierType
	SelectItems   []*operator.SelectItemNode
	Having        *operator.BooleanExpressionNode
	IsAggregate   bool
}

func NewSelectNode(runtime *config.Runtime, input Node, sq parser.ISetQuantifierContext, items []parser.ISelectItemContext, having parser.IBooleanExpressionContext) *SelectNode {
	res := &SelectNode{
		Input:         input,
		Metadata:      metadata.NewMetadata(),
		SetQuantifier: nil,
		SelectItems:   []*operator.SelectItemNode{},
		Having:        nil,
	}
	if sq != nil {
		q := datatype.ToQuantifierType(sq.GetText())
		res.SetQuantifier = &q
	}
	for i := 0; i < len(items); i++ {
		itemNode := operator.NewSelectItemNode(runtime, items[i])
		res.SelectItems = append(res.SelectItems, itemNode)
		if itemNode.IsAggregate() {
			res.IsAggregate = true
		}
	}

	if having != nil {
		res.Having = operator.NewBooleanExpressionNode(runtime, having)
		if res.Having.IsAggregate() {
			res.IsAggregate = true
		}
	}
	return res
}

func (n *SelectNode) GetType() NodeType {
	return NodeTypeSelect
}

func (n *SelectNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *SelectNode) SetInputs(inputs []Node) {
	n.Input = inputs[0]
}

func (n *SelectNode) GetOutput() Node {
	return n.Output
}

func (n *SelectNode) SetOutput(output Node) {
	n.Output = output
}

func (n *SelectNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *SelectNode) SetMetadata() error {
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

func (n *SelectNode) String() string {
	res := "SelectNode {\n"
	res += "Input: " + n.Input.String() + "\n"
	res += "Metadata: " + fmt.Sprint(n.Metadata) + "\n"
	res += "SelectItems: " + fmt.Sprint(n.SelectItems) + "\n"
	res += "}\n"
	return res
}
