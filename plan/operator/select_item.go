package operator

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type SelectItemNode struct {
	Expression    *ExpressionNode
	QualifiedName *QualifiedNameNode
	Identifier    *IdentifierNode
	Names         []string
}

func NewSelectItemNode(runtime *config.Runtime, t parser.ISelectItemContext) *SelectItemNode {
	res := &SelectItemNode{}
	tt := t.(*parser.SelectItemContext)
	if id := tt.Identifier(); id != nil {
		res.Identifier = NewIdentifierNode(runtime, id)
	}

	if ep := tt.Expression(); ep != nil {
		res.Expression = NewExpressionNode(runtime, ep)
		res.Names = []string{res.Expression.Name}

	} else if qn := tt.QualifiedName(); qn != nil {
		res.QualifiedName = NewQualifiedNameNode(runtime, qn)
	}

	if res.Identifier != nil {
		res.Names = []string{tt.Identifier().(*parser.IdentifierContext).GetText()}
	}
	return res
}

func (n *SelectItemNode) GetNames() []string {
	return n.Names
}

func (n *SelectItemNode) GetNamesAndTypes(md *metadata.Metadata) ([]string, []gtype.Type, error) {
	var types []gtype.Type
	if n.Expression != nil {
		t, err := n.Expression.GetType(md)
		if err != nil {
			return n.Names, types, err
		}
		types = append(types, t)
		return n.Names, types, nil

	} else {
		return md.GetColumnNames(), md.GetColumnTypes(), nil
	}
}

func (n *SelectItemNode) GetColumns(md *metadata.Metadata) ([]string, error) {
	if n.Expression != nil {
		return n.Expression.GetColumns()
	} else { //*
		return md.GetColumnNames(), nil
	}
}

func (n *SelectItemNode) Init(md *metadata.Metadata) error {
	if n.Expression != nil { //some items
		if err := n.Expression.Init(md); err != nil {
			return err
		}

	}
	return nil
}

func (n *SelectItemNode) ExtractAggFunc(res *[]*FuncCallNode) {
	if n.Expression != nil { //some items
		n.Expression.ExtractAggFunc(res)
	} else { //*
	}
}

func (n *SelectItemNode) Result(input *row.RowsGroup) ([]interface{}, error) {
	res := []interface{}{}
	if n.Expression != nil { //some items
		rec, err := n.Expression.Result(input)
		if err != nil {
			return res, err
		}
		res = append(res, rec)

	} else { //*
		for _, v := range input.Vals {
			res = append(res, v)
		}
		n.Names = input.Metadata.GetColumnNames()
	}

	return res, nil
}

func (n *SelectItemNode) IsAggregate() bool {
	if n.Expression != nil {
		return n.Expression.IsAggregate()
	}
	return false
}
