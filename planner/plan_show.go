package planner

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/datatype"
	"github.com/gotodb/gotodb/metadata"
)

type ShowNodeType int32

const (
	_ ShowNodeType = iota
	ShowCatalogs
	ShowSchemas
	ShowTables
	ShowColumns
	ShowStats
	ShowPartitions
	ShowCreateTable
	ShowCreateView
)

func (s ShowNodeType) String() string {
	switch s {
	case ShowCatalogs:
		return "SHOWCATALOGS"
	case ShowSchemas:
		return "SHOWSCHEMAS"
	case ShowTables:
		return "SHOWTABLES"
	case ShowColumns:
		return "SHOWCOLUMNS"
	case ShowStats:
		return "SHOWSTATS"
	case ShowPartitions:
		return "SHOWPARTITIONS"
	case ShowCreateTable:
		return "SHOWCREATETABLE"
	case ShowCreateView:
		return "SHOWCREATEVIEW"
	}
	return "UNKNOWNSHOWTYPE"
}

type ShowPlan struct {
	Input    Plan
	Output   Plan
	Metadata *metadata.Metadata
	ShowType ShowNodeType

	//show catalogs/schemas/tables/columns/create table/create view
	Catalog     string
	Schema      string
	Table       string
	LikePattern *string
	Escape      *string
}

func NewShowPlanTables(_ *config.Runtime, catalog, schema string, like, escape *string) *ShowPlan {
	return &ShowPlan{
		ShowType:    ShowTables,
		Catalog:     catalog,
		Schema:      schema,
		LikePattern: like,
		Escape:      escape,
	}
}

func NewShowPlanCatalogs(_ *config.Runtime, like, escape *string) *ShowPlan {
	return &ShowPlan{
		ShowType:    ShowCatalogs,
		LikePattern: like,
		Escape:      escape,
	}
}

func NewShowPlanSchemas(_ *config.Runtime, catalog string, like, escape *string) *ShowPlan {
	return &ShowPlan{
		ShowType:    ShowSchemas,
		Catalog:     catalog,
		LikePattern: like,
		Escape:      escape,
	}
}

func NewShowPlanColumns(_ *config.Runtime, catalog, schema, table string) *ShowPlan {
	return &ShowPlan{
		ShowType: ShowColumns,
		Catalog:  catalog,
		Schema:   schema,
		Table:    table,
	}
}

func NewShowPlanPartitions(_ *config.Runtime, catalog, schema, table string) *ShowPlan {
	return &ShowPlan{
		ShowType: ShowPartitions,
		Catalog:  catalog,
		Schema:   schema,
		Table:    table,
	}
}

func (n *ShowPlan) GetType() NodeType {
	return NodeTypeShow
}

func (n *ShowPlan) SetMetadata() error {
	res := metadata.NewMetadata()
	switch n.ShowType {
	case ShowCatalogs:
		col := metadata.NewColumnMetadata(datatype.STRING, "*", "*", "*", "catalog")
		res.AppendColumn(col)
	case ShowTables:
		col := metadata.NewColumnMetadata(datatype.STRING, n.Catalog, n.Schema, "*", "table")
		res.AppendColumn(col)
	case ShowSchemas:
		col := metadata.NewColumnMetadata(datatype.STRING, n.Catalog, "*", "*", "schema")
		res.AppendColumn(col)
	case ShowColumns:
		col := metadata.NewColumnMetadata(datatype.STRING, n.Catalog, n.Schema, n.Table, "NAME")
		res.AppendColumn(col)
		col = metadata.NewColumnMetadata(datatype.STRING, n.Catalog, n.Schema, n.Table, "TYPE")
		res.AppendColumn(col)
	case ShowPartitions:
		ctr, err := connector.NewConnector(n.Catalog, n.Schema, n.Table)
		if err != nil {
			return err
		}
		parInfo, err := ctr.GetPartition(1)
		if err != nil {
			return err
		}
		res = parInfo.Metadata
	}

	n.Metadata = res

	return nil
}

func (n *ShowPlan) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *ShowPlan) GetOutput() Plan {
	return n.Output
}

func (n *ShowPlan) SetOutput(output Plan) {
	n.Output = output
}

func (n *ShowPlan) GetInputs() []Plan {
	return []Plan{n.Input}
}

func (n *ShowPlan) SetInputs(_ []Plan) {
}

func (n *ShowPlan) String() string {
	res := "ShowPlan {\n"
	res += "ShowType: " + n.ShowType.String() + "\n"
	res += "Catalog: " + n.Catalog + "\n"
	res += "Schema: " + n.Schema + "\n"
	res += "LikePattern: " + *n.LikePattern + "\n"
	res += "Escape: " + *n.Escape + "\n"
	res += "}\n"
	return res
}
