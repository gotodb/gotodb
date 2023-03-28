package optimizer

import (
	"github.com/gotodb/gotodb/plan"
)

func DeleteRenameNode(node plan.Node) error {
	if node == nil {
		return nil
	}
	switch node.(type) {
	case *plan.RenameNode:
		renameNode := node.(*plan.RenameNode)
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

	default:
		for _, input := range node.GetInputs() {
			if err := DeleteRenameNode(input); err != nil {
				return err
			}
		}
	}
	return nil
}
