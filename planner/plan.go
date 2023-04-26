package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/pkg/parser"
	"github.com/gotodb/gotodb/planner/operator"
	"strings"
)

type NodeType int32

const (
	_ NodeType = iota
	NodeTypeScan
	NodeTypeJoin
	NodeTypeHashJoin
	NodeTypeFilter
	NodeTypeOrderBy
	NodeTypeLimit
	NodeTypeSelect
	NodeTypeUnion
	NodeTypeHaving
	NodeTypeRename
	NodeTypeCombine
	NodeTypeGroupBy
	NodeTypeAggregate
	NodeTypeAggregateFuncLocal
	NodeTypeAggregateFuncGlobal
	NodeTypeShuffle
	NodeTypeDistinctLocal
	NodeTypeDistinctGlobal
	NodeTypeUse
	NodeTypeShow
)

type Plan interface {
	GetType() NodeType
	SetMetadata() error
	GetMetadata() *metadata.Metadata

	GetOutput() Plan
	SetOutput(output Plan)

	GetInputs() []Plan
	SetInputs(input []Plan)

	String() string
}

func NewPlanFromSingleStatement(runtime *config.Runtime, t parser.ISingleStatementContext) Plan {
	return NewPlanFromStatement(runtime, t.(*parser.SingleStatementContext).Statement())
}

func NewPlanFromStatement(runtime *config.Runtime, t parser.IStatementContext) Plan {
	tt := t.(*parser.StatementContext)

	if tt.INSERT() != nil {
		return NewPlanFromInsert(runtime, tt)
	}

	if tt.Query() != nil {
		return NewPlanFromQuery(runtime, tt.Query())
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

		return NewUsePlan(runtime, catalog, schema)
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
		return NewShowPlanTables(runtime, catalog, schema, like, escape)
	}

	//show schemas
	if tt.SHOW() != nil && tt.CATALOGS() != nil {
		var like, escape string

		if tt.LIKE() != nil {
			stringValue := operator.NewStringValueNode(runtime, tt.GetPattern())
			like = stringValue.Str
		}

		if tt.ESCAPE() != nil {
			stringValue := operator.NewStringValueNode(runtime, tt.GetEscape())
			escape = stringValue.Str
		}

		return NewShowPlanCatalogs(runtime, &like, &escape)
	}

	if tt.SHOW() != nil && tt.SCHEMAS() != nil {
		catalog := runtime.Catalog
		if id := tt.Identifier(0); id != nil {
			catalog = operator.NewIdentifierNode(runtime, id).GetText()
		}
		var like, escape *string
		return NewShowPlanSchemas(runtime, catalog, like, escape)
	}

	//show columns
	if (tt.SHOW() != nil && tt.COLUMNS() != nil) || (tt.DESC() != nil || tt.DESCRIBE() != nil) {
		catalog, schema, table := runtime.Catalog, runtime.Schema, runtime.Table
		if qname := tt.QualifiedName(); qname != nil {
			name := operator.NewQualifiedNameNode(runtime, qname).Result()
			catalog, schema, table = metadata.SplitTableName(runtime, name)
		}
		return NewShowPlanColumns(runtime, catalog, schema, table)
	}

	//show partitions
	if tt.SHOW() != nil && tt.PARTITIONS() != nil {
		catalog, schema, table := runtime.Catalog, runtime.Schema, runtime.Table
		if qname := tt.QualifiedName(); qname != nil {
			name := operator.NewQualifiedNameNode(runtime, qname).Result()
			catalog, schema, table = metadata.SplitTableName(runtime, name)
		}
		var res Plan
		res = NewShowPlanPartitions(runtime, catalog, schema, table)
		if wh := tt.GetWhere(); wh != nil {
			filterNode := NewFilterPlan(runtime, res, wh)
			res.SetOutput(filterNode)
			res = filterNode
		}

		if tt.ORDER() != nil {
			orderNode := NewOrderByPlan(runtime, res, tt.AllSortItem())
			res.SetOutput(orderNode)
			res = orderNode
		}
		if tt.LIMIT() != nil {
			if iv := tt.INTEGER_VALUE(); iv != nil {
				limitNode := NewLimitPlan(runtime, res, iv)
				res.SetOutput(limitNode)
				res = limitNode
			} else if ia := tt.ALL(); ia != nil {
				limitNode := NewLimitPlan(runtime, res, ia)
				res.SetOutput(limitNode)
				res = limitNode
			}
		}
		return res
	}

	return nil
}

func NewPlanFromInsert(runtime *config.Runtime, t parser.IStatementContext) Plan {
	var res Plan

	queryNode := NewPlanFromQuery(runtime, t.Query())
	res = queryNode

	insertNode := NewInsertPlan(runtime, res, t.QualifiedName(), t.ColumnAliases())
	res.SetOutput(insertNode)
	res = insertNode

	return res
}

func NewPlanFromQuery(runtime *config.Runtime, t parser.IQueryContext) Plan {
	tt := t.(*parser.QueryContext)
	var res Plan
	queryNode := NewPlanFromQueryTerm(runtime, tt.QueryTerm())
	res = queryNode

	if tt.ORDER() != nil {
		orderNode := NewOrderByPlan(runtime, res, tt.AllSortItem())
		res.SetOutput(orderNode)
		res = orderNode
	}

	if tt.LIMIT() != nil {
		if iv := tt.INTEGER_VALUE(); iv != nil {
			limitNode := NewLimitPlan(runtime, res, iv)
			res.SetOutput(limitNode)
			res = limitNode
		} else if ia := tt.ALL(); ia != nil {
			limitNode := NewLimitPlan(runtime, res, ia)
			res.SetOutput(limitNode)
			res = limitNode
		}
	}
	return res
}

func NewPlanFromQueryTerm(runtime *config.Runtime, t parser.IQueryTermContext) Plan {
	var res Plan
	tt := t.(*parser.QueryTermContext)
	if tqp := tt.QueryPrimary(); tqp != nil {
		res = NewPlanFromQueryPrimary(runtime, tqp)

	} else {
		left := NewPlanFromQueryTerm(runtime, tt.GetLeft())
		right := NewPlanFromQueryTerm(runtime, tt.GetRight())
		op := tt.GetOperator()
		unionNode := NewUnionPlan(runtime, left, right, op)
		left.SetOutput(unionNode)
		right.SetOutput(unionNode)
		res = unionNode
	}

	return res
}

func NewPlanFromQueryPrimary(runtime *config.Runtime, t parser.IQueryPrimaryContext) Plan {
	var res Plan
	tt := t.(*parser.QueryPrimaryContext)
	if tqs := tt.QuerySpecification(); tqs != nil {
		res = NewPlanFromQuerySpecification(runtime, tqs)
	} else {
		res = NewPlanFromQuery(runtime, tt.Query())
	}
	return res
}

func NewPlanFromQuerySpecification(runtime *config.Runtime, t parser.IQuerySpecificationContext) Plan {
	tt := t.(*parser.QuerySpecificationContext)
	var res Plan
	if rels := tt.AllRelation(); rels != nil && len(rels) > 0 {
		res = NewPlanFromRelations(runtime, rels)

	}
	if wh := tt.GetWhere(); wh != nil {
		filterNode := NewFilterPlan(runtime, res, wh)
		res.SetOutput(filterNode)
		res = filterNode
	}

	if gb := tt.GroupBy(); gb != nil {
		groupByNode := NewGroupByPlan(runtime, res, gb)
		res.SetOutput(groupByNode)
		res = groupByNode
	}

	selectNode := NewSelectPlan(runtime, res, tt.SetQuantifier(), tt.AllSelectItem(), tt.GetHaving())

	res.SetOutput(selectNode)

	res = selectNode
	return res
}

func NewPlanFromSampleRelation(runtime *config.Runtime, t parser.ISampledRelationContext) Plan {
	tt := t.(*parser.SampledRelationContext)
	res := NewPlanFromRelationPrimary(runtime, tt.RelationPrimary())
	if id := tt.Identifier(); id != nil {
		idNode := operator.NewIdentifierNode(runtime, id)
		rename := idNode.GetText()
		renameNode := NewRenamePlan(runtime, res, rename)
		res.SetOutput(renameNode)
		res = renameNode
	}
	return res
}

func NewPlanFromRelationPrimary(runtime *config.Runtime, t parser.IRelationPrimaryContext) Plan {
	tt := t.(*parser.RelationPrimaryContext)
	if tn := tt.QualifiedName(); tn != nil {
		ttn := tn.(*parser.QualifiedNameContext)
		qname := ttn.GetText()
		return NewScanPlan(runtime, qname)

	} else if tq := tt.Query(); tq != nil {
		return NewPlanFromQuery(runtime, tq)

	} else if tr := tt.Relation(); tr != nil {
		return NewPlanFromRelation(runtime, tr)
	}
	return nil
}

func NewPlanFromRelation(runtime *config.Runtime, t parser.IRelationContext) Plan {
	tt := t.(*parser.RelationContext)
	if sr := tt.SampledRelation(); sr != nil {
		return NewPlanFromSampleRelation(runtime, sr)

	} else { //join
		leftRelation, rightRelation := t.GetLeftRelation(), t.GetRightRelation()
		leftNode, rightNode := NewPlanFromRelation(runtime, leftRelation), NewPlanFromRelation(runtime, rightRelation)
		joinText := tt.JoinType().(*parser.JoinTypeContext).GetText()
		var joinType JoinType
		if joinText == "" || joinText[0:1] == "I" {
			joinType = InnerJoin
		} else if joinText[0:1] == "L" {
			joinType = LeftJoin
		} else if joinText[0:1] == "R" {
			joinType = RightJoin
		}
		joinCriteriaNode := operator.NewJoinCriteriaNode(runtime, tt.JoinCriteria())
		res := NewJoinPlan(runtime, leftNode, rightNode, joinType, joinCriteriaNode)
		leftNode.SetOutput(res)
		rightNode.SetOutput(res)
		return res
	}
}

func NewPlanFromRelations(runtime *config.Runtime, ts []parser.IRelationContext) Plan {
	if len(ts) == 1 {
		return NewPlanFromRelation(runtime, ts[0])
	}
	res := NewCombinePlan(runtime, []Plan{})
	for _, t := range ts {
		relationNode := NewPlanFromRelation(runtime, t)
		res.Inputs = append(res.Inputs, relationNode)
		relationNode.SetOutput(res)
	}
	return res
}
