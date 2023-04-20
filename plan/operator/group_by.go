package operator

import (
	"fmt"
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/row"
)

type GroupByNode struct {
	GroupingElements []*GroupingElementNode
}

func NewGroupByNode(runtime *config.Runtime, t parser.IGroupByContext) *GroupByNode {
	if t == nil {
		return nil
	}
	res := &GroupByNode{
		GroupingElements: []*GroupingElementNode{},
	}
	tt := t.(*parser.GroupByContext)
	elements := tt.AllGroupingElement()
	for _, element := range elements {
		res.GroupingElements = append(res.GroupingElements, NewGroupingElementNode(runtime, element))
	}
	return res
}

func (n *GroupByNode) Init(md *metadata.Metadata) error {
	for _, element := range n.GroupingElements {
		if err := element.Init(md); err != nil {
			return err
		}
	}
	return nil
}

func (n *GroupByNode) Result(input *row.RowsGroup) ([]interface{}, error) {
	rn := input.GetRowsNumber()
	res := make([]interface{}, rn)
	for i := 0; i < rn; i++ {
		res[i] = ""
	}

	for _, element := range n.GroupingElements {
		esi, err := element.Result(input)
		if err != nil {
			return nil, err
		}
		es := esi.([]interface{})
		for i, e := range es {
			res[i] = res[i].(string) + fmt.Sprintf("%v:", e)
		}
	}
	return res, nil
}

func (n *GroupByNode) GetColumns() ([]string, error) {
	var res []string
	for _, ele := range n.GroupingElements {
		cs, err := ele.GetColumns()
		if err != nil {
			return res, err
		}
		res = append(res, cs...)
	}
	return res, nil
}
