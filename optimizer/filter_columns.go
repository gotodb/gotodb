package optimizer

import (
	"fmt"
	"sort"

	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/planner"
)

func FilterColumns(node planner.Plan, columns []string) error {
	if node == nil {
		return nil
	}
	switch node.(type) {
	case *planner.InsertPlan, *planner.JoinPlan, *planner.LimitPlan, *planner.UnionPlan, *planner.CombinePlan, *planner.AggregatePlan, *planner.AggregateFuncLocalPlan:
		var indexes []int
		var md = node.GetMetadata()
		//for join node
		if joinNode, ok := node.(*planner.JoinPlan); ok {
			cs, err := joinNode.JoinCriteria.GetColumns()
			if err != nil {
				return err
			}
			columns = append(columns, cs...)
		}
		for _, c := range columns {
			if index, err := md.GetIndexByName(c); err == nil {
				indexes = append(indexes, index)
			}
		}
		sort.Ints(indexes)

		var inputs = node.GetInputs()
		var inputMDs []*metadata.Metadata
		for _, input := range inputs {
			inputMDs = append(inputMDs, input.GetMetadata())
		}

		var columnsForInput = make([][]string, len(inputs))
		var indexNum = inputMDs[0].GetColumnNumber()
		var i = 0
		for j := 0; j < len(indexes); j++ {
			index := indexes[j]
			if index < indexNum {
				indexForInput := index - (indexNum - inputMDs[i].GetColumnNumber())
				cname := inputMDs[i].Columns[indexForInput].GetName()
				columnsForInput[i] = append(columnsForInput[i], cname)
			} else {
				i++
				indexNum += inputMDs[i].GetColumnNumber()
				j--
			}
		}
		for i, input := range inputs {
			err := FilterColumns(input, columnsForInput[i])
			if err != nil {
				return err
			}
		}

	case *planner.FilterPlan:
		filterNode := node.(*planner.FilterPlan)
		var columnsForInput []string
		for _, be := range filterNode.BooleanExpressions {
			cols, err := be.GetColumns()
			if err != nil {
				return err
			}
			columnsForInput = append(columnsForInput, cols...)

			if be.IsSetSubQuery() {
				err = FilterColumns(be.Predicated.Predicate.QueryPlan, columnsForInput)
				if err != nil {
					return err
				}
			}
		}
		columnsForInput = append(columnsForInput, columns...)
		return FilterColumns(filterNode.Input, columnsForInput)

	case *planner.GroupByPlan:
		groupByNode := node.(*planner.GroupByPlan)
		columnsForInput, err := groupByNode.GroupBy.GetColumns()
		if err != nil {
			return err
		}
		columnsForInput = append(columnsForInput, columns...)
		return FilterColumns(groupByNode.Input, columnsForInput)

	case *planner.OrderByPlan:
		orderByNode := node.(*planner.OrderByPlan)
		columnsForInput := columns
		for _, item := range orderByNode.SortItems {
			cs, err := item.GetColumns()
			if err != nil {
				return err
			}
			columnsForInput = append(columnsForInput, cs...)
		}
		return FilterColumns(orderByNode.Input, columnsForInput)

	case *planner.SelectPlan:
		selectNode := node.(*planner.SelectPlan)
		columnsForInput := columns
		for _, item := range selectNode.SelectItems {
			cs, err := item.GetColumns(selectNode.Input.GetMetadata())
			if err != nil {
				return err
			}
			columnsForInput = append(columnsForInput, cs...)
		}
		if selectNode.Having != nil {
			cs, err := selectNode.Having.GetColumns()
			if err != nil {
				return err
			}
			columnsForInput = append(columnsForInput, cs...)
		}
		return FilterColumns(selectNode.Input, columnsForInput)

	case *planner.ScanPlan:
		scanNode := node.(*planner.ScanPlan)
		scanNode.Metadata = scanNode.Metadata.SelectColumns(columns)
		parent := scanNode.GetOutput()

		for parent != nil {
			_ = parent.SetMetadata()
			parent = parent.GetOutput()
		}
		return nil

	case *planner.ShowPlan:
		return nil

	case *planner.RenamePlan: //already use deleteRenameNode
		return nil

	default:
		return fmt.Errorf("[Optimizer:FilterColumns]Unknown PlanNode type")
	}

	return nil
}
