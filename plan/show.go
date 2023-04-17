package plan

import (
	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/connector"
	"github.com/gotodb/gotodb/gtype"
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

type ShowNode struct {
	Input    Node
	Output   Node
	Metadata *metadata.Metadata
	ShowType ShowNodeType

	//show catalogs/schemas/tables/columns/create table/create view
	Catalog     string
	Schema      string
	Table       string
	LikePattern *string
	Escape      *string
}

func NewShowNodeTables(_ *config.Runtime, catalog, schema string, like, escape *string) *ShowNode {
	return &ShowNode{
		ShowType:    ShowTables,
		Catalog:     catalog,
		Schema:      schema,
		LikePattern: like,
		Escape:      escape,
	}
}

func NewShowNodeSchemas(_ *config.Runtime, catalog string, like, escape *string) *ShowNode {
	return &ShowNode{
		ShowType:    ShowSchemas,
		Catalog:     catalog,
		LikePattern: like,
		Escape:      escape,
	}
}

func NewShowNodeColumns(_ *config.Runtime, catalog, schema, table string) *ShowNode {
	return &ShowNode{
		ShowType: ShowColumns,
		Catalog:  catalog,
		Schema:   schema,
		Table:    table,
	}
}

func NewShowNodePartitions(_ *config.Runtime, catalog, schema, table string) *ShowNode {
	return &ShowNode{
		ShowType: ShowPartitions,
		Catalog:  catalog,
		Schema:   schema,
		Table:    table,
	}
}

func (n *ShowNode) GetType() NodeType {
	return NodeTypeShow
}

func (n *ShowNode) SetMetadata() error {
	res := metadata.NewMetadata()
	switch n.ShowType {
	case ShowCatalogs:
	case ShowTables:
		col := metadata.NewColumnMetadata(gtype.STRING, n.Catalog, n.Schema, "*", "table")
		res.AppendColumn(col)
	case ShowSchemas:
		col := metadata.NewColumnMetadata(gtype.STRING, n.Catalog, "*", "*", "schema")
		res.AppendColumn(col)
	case ShowColumns:
		col := metadata.NewColumnMetadata(gtype.STRING, n.Catalog, n.Schema, n.Table, "NAME")
		res.AppendColumn(col)
		col = metadata.NewColumnMetadata(gtype.STRING, n.Catalog, n.Schema, n.Table, "TYPE")
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

func (n *ShowNode) GetMetadata() *metadata.Metadata {
	return n.Metadata
}

func (n *ShowNode) GetOutput() Node {
	return n.Output
}

func (n *ShowNode) SetOutput(output Node) {
	n.Output = output
}

func (n *ShowNode) GetInputs() []Node {
	return []Node{n.Input}
}

func (n *ShowNode) SetInputs(_ []Node) {
}

func (n *ShowNode) String() string {
	res := "ShowNode {\n"
	res += "ShowType: " + n.ShowType.String() + "\n"
	res += "Catalog: " + n.Catalog + "\n"
	res += "Schema: " + n.Schema + "\n"
	res += "LikePattern: " + *n.LikePattern + "\n"
	res += "Escape: " + *n.Escape + "\n"
	res += "}\n"
	return res
}
