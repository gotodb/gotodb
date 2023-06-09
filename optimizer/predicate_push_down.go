package optimizer

import (
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/planner"
)

func ExtractPredicates(node *planner.BooleanExpressionNode, t datatype.Operator) []*planner.BooleanExpressionNode {
	var res []*planner.BooleanExpressionNode
	if node.Predicated != nil {
		if !node.IsSetSubQuery() {
			res = append(res, node)
		}

	} else if node.NotBooleanExpression != nil {
		res = append(res, node)

	} else if node.BinaryBooleanExpression != nil {
		leftNode := node.BinaryBooleanExpression.LeftBooleanExpression
		rightNode := node.BinaryBooleanExpression.RightBooleanExpression

		if *(node.BinaryBooleanExpression.Operator) == t {
			leftRes := ExtractPredicates(leftNode, t)
			rightRes := ExtractPredicates(rightNode, t)
			res = append(res, leftRes...)
			res = append(res, rightRes...)

		} else {
			res = append(res, node)
		}
	}
	return res
}

func PredicatePushDown(node planner.Plan, predicates []*planner.BooleanExpressionNode) error {
	if node == nil {
		return nil
	}

	switch node.(type) {
	case *planner.FilterPlan:
		filterNode := node.(*planner.FilterPlan)
		for _, be := range filterNode.BooleanExpressions {
			predicates = append(predicates, ExtractPredicates(be, datatype.AND)...)
			if be.IsSetSubQuery() {
				if err := PredicatePushDown(be.Predicated.Predicate.QueryPlan, []*planner.BooleanExpressionNode{}); err != nil {
					return err
				}
			}
		}

		inputs := filterNode.GetInputs()
		for _, input := range inputs {
			var predicatesForInput []*planner.BooleanExpressionNode
			for _, predicate := range predicates {
				md := input.GetMetadata()
				cols, err := predicate.GetColumns()
				if err != nil {
					return err
				}

				if md.Contains(cols) {
					predicatesForInput = append(predicatesForInput, predicate)
				}
			}
			if len(predicatesForInput) > 0 {
				if err := PredicatePushDown(input, predicatesForInput); err != nil {
					return err
				}
			}
		}

	case *planner.SelectPlan:
		selectNode := node.(*planner.SelectPlan)
		md := selectNode.GetMetadata()

		var res []*planner.BooleanExpressionNode
		for _, predicate := range predicates {
			cols, err := predicate.GetColumns()
			if err != nil {
				return err
			}
			if md.Contains(cols) {
				res = append(res, predicate)
			}
		}
		if len(res) > 0 {
			output := selectNode.GetOutput()
			if _, ok := output.(*planner.FilterPlan); !ok {
				newFilterNode := &planner.FilterPlan{
					Input:              node,
					Output:             output,
					Metadata:           node.GetMetadata().Copy(),
					BooleanExpressions: []*planner.BooleanExpressionNode{},
				}
				output.SetInputs([]planner.Plan{newFilterNode})
				node.SetOutput(newFilterNode)
			}
			outputNode := selectNode.GetOutput().(*planner.FilterPlan)
			outputNode.AddBooleanExpressions(res...)
		}

		for _, input := range node.GetInputs() {
			if err := PredicatePushDown(input, []*planner.BooleanExpressionNode{}); err != nil {
				return err
			}
		}

		return nil

	case *planner.ScanPlan:
		scanNode := node.(*planner.ScanPlan)
		md := node.GetMetadata()
		for _, predicate := range predicates {
			cols, err := predicate.GetColumns()
			if err != nil {
				return err
			}
			if md.Contains(cols) {
				scanNode.Filters = append(scanNode.Filters, predicate)
			}
		}
	case *planner.ShowPlan:
		return nil

	default:
		inputs := node.GetInputs()
		for _, input := range inputs {
			if len(predicates) <= 0 {
				if err := PredicatePushDown(input, predicates); err != nil {
					return err
				}
				continue
			}
			var predicatesForInput []*planner.BooleanExpressionNode
			for _, predicate := range predicates {
				md := input.GetMetadata()
				cols, err := predicate.GetColumns()
				if err != nil {
					return err
				}

				if md.Contains(cols) {
					predicatesForInput = append(predicatesForInput, predicate)
				}
			}

			if err := PredicatePushDown(input, predicatesForInput); err != nil {
				return err
			}
		}
	}
	return nil
}
