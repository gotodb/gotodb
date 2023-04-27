package optimizer

import (
	"github.com/gotodb/gotodb/planner"
)

func DeleteRenameNode(node planner.Plan) error {
	if node == nil {
		return nil
	}
	switch node.(type) {
	case *planner.RenamePlan:
		renameNode := node.(*planner.RenamePlan)
		if err := DeleteRenameNode(renameNode.Input); err != nil {
			return err
		}
		parent := renameNode.Output
		md := renameNode.Input.GetMetadata()
		md.Rename(renameNode.Rename)

		renameNode.Input.SetOutput(parent)

		parInputs := parent.GetInputs()
		i := 0
		for i = 0; i < len(parInputs); i++ {
			if parInputs[i] == node {
				break
			}
		}
		parInputs[i] = renameNode.Input
		parent.SetInputs(parInputs)

		return nil
	case *planner.FilterPlan:
		filterNode := node.(*planner.FilterPlan)
		for _, be := range filterNode.BooleanExpressions {
			if be.IsSetSubQuery() {
				if err := DeleteRenameNode(be.Predicated.Predicate.QueryPlan); err != nil {
					return err
				}
			}
		}
		for _, input := range node.GetInputs() {
			if err := DeleteRenameNode(input); err != nil {
				return err
			}
		}
	default:
		for _, input := range node.GetInputs() {
			if err := DeleteRenameNode(input); err != nil {
				return err
			}
		}
	}
	return nil
}
