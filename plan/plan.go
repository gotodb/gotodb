package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/plan/operator"
	"strings"
)

type NodeType int32

const (
	_ NodeType = iota
	SCANNODE
	JOINNODE
	HASHJOINNODE
	FILTERNODE
	ORDERBYNODE
	LIMITNODE
	SELECTNODE
	UNIONNODE
	HAVINGNODE
	RENAMENODE
	COMBINENODE
	GROUPBYNODE
	AGGREGATENODE
	AGGREGATEFUNCLOCALNODE
	AGGREGATEFUNCGLOBALNODE
	SHUFFLENODE
	DISTINCTLOCALNODE
	DISTINCTGLOBALNODE

	USENODE
	SHOWNODE
)

type Node interface {
	GetType() NodeType
	SetMetadata() error
	GetMetadata() *metadata.Metadata

	GetOutput() Node
	SetOutput(output Node)

	GetInputs() []Node
	SetInputs(input []Node)

	String() string
}

func NewNodeFromSingleStatement(runtime *config.Runtime, t parser.ISingleStatementContext) Node {
	return NewNodeFromStatement(runtime, t.(*parser.SingleStatementContext).Statement())
}

func NewNodeFromStatement(runtime *config.Runtime, t parser.IStatementContext) Node {
	tt := t.(*parser.StatementContext)
	if tt.Query() != nil {
		return NewNodeFromQuery(runtime, tt.Query())
	}

	//use
	if tt.USE() != nil {
		catalog, schema := runtime.Catalog, runtime.Schema

		if ct := tt.GetCatalog(); ct != nil {
			catalogNode := operator.NewIdentifierNode(runtime, ct)
			catalog = catalogNode.GetText()
		}

		if sc := tt.GetSchema(); sc != nil {
			schemaNode := operator.NewIdentifierNode(runtime, sc)
			schema = schemaNode.GetText()
		}

		return NewUseNode(runtime, catalog, schema)
	}

	//show tables
	if tt.SHOW() != nil && tt.TABLES() != nil {
		catalog, schema := runtime.Catalog, runtime.Schema
		if qname := tt.QualifiedName(); qname != nil {
			name := operator.NewQualifiedNameNode(runtime, qname).Result()
			names := strings.Split(name, ".")
			if len(names) == 1 {
				schema = names[0]
			} else if len(names) == 2 {
				catalog = names[0]
				schema = names[1]
			}
		}
		var like, escape *string
		return NewShowNodeTables(runtime, catalog, schema, like, escape)
	}

	//show schemas
	if tt.SHOW() != nil && tt.SCHEMAS() != nil {
		catalog := runtime.Catalog
		if id := tt.Identifier(0); id != nil {
			catalog = operator.NewIdentifierNode(runtime, id).GetText()
		}
		var like, escape *string
		return NewShowNodeSchemas(runtime, catalog, like, escape)
	}

	//show columns
	if (tt.SHOW() != nil && tt.COLUMNS() != nil) || (tt.DESC() != nil || tt.DESCRIBE() != nil) {
		catalog, schema, table := runtime.Catalog, runtime.Schema, runtime.Table
		if qname := tt.QualifiedName(); qname != nil {
			name := operator.NewQualifiedNameNode(runtime, qname).Result()
			catalog, schema, table = metadata.SplitTableName(runtime, name)
		}
		return NewShowNodeColumns(runtime, catalog, schema, table)
	}

	//show partitions
	if tt.SHOW() != nil && tt.PARTITIONS() != nil {
		catalog, schema, table := runtime.Catalog, runtime.Schema, runtime.Table
		if qname := tt.QualifiedName(); qname != nil {
			name := operator.NewQualifiedNameNode(runtime, qname).Result()
			catalog, schema, table = metadata.SplitTableName(runtime, name)
		}
		var res Node
		res = NewShowNodePartitions(runtime, catalog, schema, table)
		if wh := tt.GetWhere(); wh != nil {
			filterNode := NewFilterNode(runtime, res, wh)
			res.SetOutput(filterNode)
			res = filterNode
		}

		if tt.ORDER() != nil {
			orderNode := NewOrderByNode(runtime, res, tt.AllSortItem())
			res.SetOutput(orderNode)
			res = orderNode
		}
		if tt.LIMIT() != nil {
			if iv := tt.INTEGER_VALUE(); iv != nil {
				limitNode := NewLimitNode(runtime, res, iv)
				res.SetOutput(limitNode)
				res = limitNode
			} else if ia := tt.ALL(); ia != nil {
				limitNode := NewLimitNode(runtime, res, ia)
				res.SetOutput(limitNode)
				res = limitNode
			}
		}
		return res
	}

	return nil
}

func NewNodeFromQuery(runtime *config.Runtime, t parser.IQueryContext) Node {
	tt := t.(*parser.QueryContext)
	var res Node
	queryNode := NewNodeFromQueryTerm(runtime, tt.QueryTerm())
	res = queryNode

	if tt.ORDER() != nil {
		orderNode := NewOrderByNode(runtime, res, tt.AllSortItem())
		res.SetOutput(orderNode)
		res = orderNode
	}

	if tt.LIMIT() != nil {
		if iv := tt.INTEGER_VALUE(); iv != nil {
			limitNode := NewLimitNode(runtime, res, iv)
			res.SetOutput(limitNode)
			res = limitNode
		} else if ia := tt.ALL(); ia != nil {
			limitNode := NewLimitNode(runtime, res, ia)
			res.SetOutput(limitNode)
			res = limitNode
		}
	}
	return res
}

func NewNodeFromQueryTerm(runtime *config.Runtime, t parser.IQueryTermContext) Node {
	var res Node
	tt := t.(*parser.QueryTermContext)
	if tqp := tt.QueryPrimary(); tqp != nil {
		res = NewNodeFromQueryPrimary(runtime, tqp)

	} else {
		left := NewNodeFromQueryTerm(runtime, tt.GetLeft())
		right := NewNodeFromQueryTerm(runtime, tt.GetRight())
		op := tt.GetOperator()
		unionNode := NewUnionNode(runtime, left, right, op)
		left.SetOutput(unionNode)
		right.SetOutput(unionNode)
		res = unionNode
	}

	return res
}

func NewNodeFromQueryPrimary(runtime *config.Runtime, t parser.IQueryPrimaryContext) Node {
	var res Node
	tt := t.(*parser.QueryPrimaryContext)
	if tqs := tt.QuerySpecification(); tqs != nil {
		res = NewNodeFromQuerySpecification(runtime, tqs)
	} else {
		res = NewNodeFromQuery(runtime, tt.Query())
	}
	return res
}

func NewNodeFromQuerySpecification(runtime *config.Runtime, t parser.IQuerySpecificationContext) Node {
	tt := t.(*parser.QuerySpecificationContext)
	var res Node
	if rels := tt.AllRelation(); rels != nil && len(rels) > 0 {
		res = NewNodeFromRelations(runtime, rels)

	}
	if wh := tt.GetWhere(); wh != nil {
		filterNode := NewFilterNode(runtime, res, wh)
		res.SetOutput(filterNode)
		res = filterNode
	}

	if gb := tt.GroupBy(); gb != nil {
		groupByNode := NewGroupByNode(runtime, res, gb)
		res.SetOutput(groupByNode)
		res = groupByNode
	}

	selectNode := NewSelectNode(runtime, res, tt.SetQuantifier(), tt.AllSelectItem(), tt.GetHaving())

	res.SetOutput(selectNode)

	res = selectNode
	return res
}

func NewNodeFromSampleRelation(runtime *config.Runtime, t parser.ISampledRelationContext) Node {
	tt := t.(*parser.SampledRelationContext)
	res := NewNodeFromRelationPrimary(runtime, tt.RelationPrimary())
	if id := tt.Identifier(); id != nil {
		idNode := operator.NewIdentifierNode(runtime, id)
		rename := idNode.GetText()
		renameNode := NewRenameNode(runtime, res, rename)
		res.SetOutput(renameNode)
		res = renameNode
	}
	return res
}

func NewNodeFromRelationPrimary(runtime *config.Runtime, t parser.IRelationPrimaryContext) Node {
	tt := t.(*parser.RelationPrimaryContext)
	if tn := tt.QualifiedName(); tn != nil {
		ttn := tn.(*parser.QualifiedNameContext)
		qname := ttn.GetText()
		return NewScanNode(runtime, qname)

	} else if tq := tt.Query(); tq != nil {
		return NewNodeFromQuery(runtime, tq)

	} else if tr := tt.Relation(); tr != nil {
		return NewNodeFromRelation(runtime, tr)
	}
	return nil
}

func NewNodeFromRelation(runtime *config.Runtime, t parser.IRelationContext) Node {
	tt := t.(*parser.RelationContext)
	if sr := tt.SampledRelation(); sr != nil {
		return NewNodeFromSampleRelation(runtime, sr)

	} else { //join
		leftRelation, rightRelation := t.GetLeftRelation(), t.GetRightRelation()
		leftNode, rightNode := NewNodeFromRelation(runtime, leftRelation), NewNodeFromRelation(runtime, rightRelation)
		joinText := tt.JoinType().(*parser.JoinTypeContext).GetText()
		var joinType JoinType
		if joinText == "" || joinText[0:1] == "I" {
			joinType = INNERJOIN
		} else if joinText[0:1] == "L" {
			joinType = LEFTJOIN
		} else if joinText[0:1] == "R" {
			joinType = RIGHTJOIN
		}
		joinCriteriaNode := operator.NewJoinCriteriaNode(runtime, tt.JoinCriteria())
		res := NewJoinNode(runtime, leftNode, rightNode, joinType, joinCriteriaNode)
		leftNode.SetOutput(res)
		rightNode.SetOutput(res)
		return res
	}
}

func NewNodeFromRelations(runtime *config.Runtime, ts []parser.IRelationContext) Node {
	if len(ts) == 1 {
		return NewNodeFromRelation(runtime, ts[0])
	}
	res := NewCombineNode(runtime, []Node{})
	for _, t := range ts {
		relationNode := NewNodeFromRelation(runtime, t)
		res.Inputs = append(res.Inputs, relationNode)
		relationNode.SetOutput(res)
	}
	return res
}
