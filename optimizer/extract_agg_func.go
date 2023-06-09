package optimizer

import (
	"fmt"
	"math/rand"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/planner"
)

func ExtractDistinctExpressions(funcs []*planner.FuncCallNode) []*planner.ExpressionNode {
	var res []*planner.ExpressionNode
	for _, f := range funcs {
		if f.SetQuantifier != nil && (*f.SetQuantifier) == datatype.DISTINCT {
			res = append(res, f.Expressions...)
			colName := fmt.Sprintf("DIST_%v_%v", len(res), rand.Int())
			f.Expressions[0].Name = colName
			f.Expressions = []*planner.ExpressionNode{
				{
					Name: colName,
					BooleanExpression: &planner.BooleanExpressionNode{
						Name: colName,
						Predicated: &planner.PredicatedNode{
							Name: colName,
							ValueExpression: &planner.ValueExpressionNode{
								Name: colName,
								PrimaryExpression: &planner.PrimaryExpressionNode{
									Name: colName,
									Identifier: &planner.IdentifierNode{
										Str: &colName,
									},
								},
							},
						},
					},
				},
			}
		}
	}
	return res
}

func ExtractAggFunc(node planner.Plan) error {
	if node == nil {
		return nil
	}
	switch node.(type) {
	case *planner.SelectPlan:
		selectNode := node.(*planner.SelectPlan)
		if selectNode.IsAggregate {
			var funcs []*planner.FuncCallNode
			for _, item := range selectNode.SelectItems {
				item.ExtractAggFunc(&funcs)
			}
			if selectNode.Having != nil {
				selectNode.Having.ExtractAggFunc(&funcs)
			}

			var nodeLocal *planner.AggregateFuncLocalPlan

			//for distinct
			distEps := ExtractDistinctExpressions(funcs)
			if len(distEps) > 0 {
				distLocalNode := planner.NewDistinctLocalPlan(nil, distEps, selectNode.Input)
				distGlobalNode := planner.NewDistinctGlobalPlan(nil, distEps, distLocalNode)
				nodeLocal = planner.NewAggregateFuncLocalPlan(nil, funcs, distGlobalNode)
			} else {
				nodeLocal = planner.NewAggregateFuncLocalPlan(nil, funcs, selectNode.Input)
			}

			_ = nodeLocal.SetMetadata()

			funcsGlobal := make([]*planner.FuncCallNode, len(funcs))
			for i, f := range funcs {
				funcsGlobal[i] = &planner.FuncCallNode{
					FuncName:   f.FuncName + "GLOBAL",
					ResColName: f.ResColName,
					Expressions: []*planner.ExpressionNode{
						{
							Name: f.ResColName,
							BooleanExpression: &planner.BooleanExpressionNode{
								Name: f.ResColName,
								Predicated: &planner.PredicatedNode{
									Name: f.ResColName,
									ValueExpression: &planner.ValueExpressionNode{
										Name: f.ResColName,
										PrimaryExpression: &planner.PrimaryExpressionNode{
											Name: f.ResColName,
											Identifier: &planner.IdentifierNode{
												Str: &f.ResColName,
											},
										},
									},
								},
							},
						},
					},
				}
			}
			nodeGlobal := planner.NewAggregateFuncGlobalPlan(nil, funcsGlobal, nodeLocal)
			selectNode.Input = nodeGlobal
			if err := selectNode.SetMetadata(); err != nil {
				return err
			}
		}
	case *planner.FilterPlan:
		filterNode := node.(*planner.FilterPlan)
		for _, be := range filterNode.BooleanExpressions {
			if be.IsSetSubQuery() {
				if err := ExtractAggFunc(be.Predicated.Predicate.QueryPlan); err != nil {
					return err
				}
			}
		}
	}

	for _, input := range node.GetInputs() {
		if err := ExtractAggFunc(input); err != nil {
			return err
		}
	}
	return nil
}
