package optimizer

import (
	"fmt"
	"github.com/gotodb/gotodb/plan/operator"
	"math/rand"

	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/plan"
)

func ExtractDistinctExpressions(funcs []*operator.FuncCallNode) []*operator.ExpressionNode {
	var res []*operator.ExpressionNode
	for _, f := range funcs {
		if f.SetQuantifier != nil && (*f.SetQuantifier) == datatype.DISTINCT {
			res = append(res, f.Expressions...)
			colName := fmt.Sprintf("DIST_%v_%v", len(res), rand.Int())
			f.Expressions[0].Name = colName
			f.Expressions = []*operator.ExpressionNode{
				{
					Name: colName,
					BooleanExpression: &operator.BooleanExpressionNode{
						Name: colName,
						Predicated: &operator.PredicatedNode{
							Name: colName,
							ValueExpression: &operator.ValueExpressionNode{
								Name: colName,
								PrimaryExpression: &operator.PrimaryExpressionNode{
									Name: colName,
									Identifier: &operator.IdentifierNode{
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

func ExtractAggFunc(node plan.Node) error {
	if node == nil {
		return nil
	}
	switch node.(type) {
	case *plan.SelectNode:
		selectNode := node.(*plan.SelectNode)
		if selectNode.IsAggregate {
			var funcs []*operator.FuncCallNode
			for _, item := range selectNode.SelectItems {
				item.ExtractAggFunc(&funcs)
			}
			if selectNode.Having != nil {
				selectNode.Having.ExtractAggFunc(&funcs)
			}

			var nodeLocal *plan.AggregateFuncLocalNode

			//for distinct
			distEps := ExtractDistinctExpressions(funcs)
			if len(distEps) > 0 {
				distLocalNode := plan.NewDistinctLocalNode(nil, distEps, selectNode.Input)
				distGlobalNode := plan.NewDistinctGlobalNode(nil, distEps, distLocalNode)
				nodeLocal = plan.NewAggregateFuncLocalNode(nil, funcs, distGlobalNode)
			} else {
				nodeLocal = plan.NewAggregateFuncLocalNode(nil, funcs, selectNode.Input)
			}

			_ = nodeLocal.SetMetadata()

			funcsGlobal := make([]*operator.FuncCallNode, len(funcs))
			for i, f := range funcs {
				funcsGlobal[i] = &operator.FuncCallNode{
					FuncName:   f.FuncName + "GLOBAL",
					ResColName: f.ResColName,
					Expressions: []*operator.ExpressionNode{
						{
							Name: f.ResColName,
							BooleanExpression: &operator.BooleanExpressionNode{
								Name: f.ResColName,
								Predicated: &operator.PredicatedNode{
									Name: f.ResColName,
									ValueExpression: &operator.ValueExpressionNode{
										Name: f.ResColName,
										PrimaryExpression: &operator.PrimaryExpressionNode{
											Name: f.ResColName,
											Identifier: &operator.IdentifierNode{
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
			nodeGlobal := plan.NewAggregateFuncGlobalNode(nil, funcsGlobal, nodeLocal)
			selectNode.Input = nodeGlobal
			if err := selectNode.SetMetadata(); err != nil {
				return err
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
