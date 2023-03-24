package operator

import (
	"fmt"

	"github.com/gotodb/gotodb/config"
	"github.com/gotodb/gotodb/gtype"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/parser"
	"github.com/gotodb/gotodb/row"
)

type IdentifierNode struct {
	Str         *string
	Digit       *int
	NonReserved *string
}

func NewIdentifierNode(_ *config.Runtime, t parser.IIdentifierContext) *IdentifierNode {
	tt := t.(*parser.IdentifierContext)
	res := &IdentifierNode{}
	var (
		str string
		dig int
	)

	if id := tt.IDENTIFIER(); id != nil {
		str = id.GetText()
		res.Str = &str

	} else if qid := tt.QUOTED_IDENTIFIER(); qid != nil {
		str = qid.GetText()
		ln := len(str)
		str = str[1 : ln-1]
		res.Str = &str

	} else if nr := tt.NonReserved(); nr != nil {
		str = nr.GetText()
		res.NonReserved = &str

	} else if did := tt.DIGIT_IDENTIFIER(); did != nil {
		str = did.GetText()
		fmt.Sscanf(str, "%d", &dig)
		res.Digit = &dig
	}
	return res
}

func (n *IdentifierNode) GetType(md *metadata.Metadata) (gtype.Type, error) {
	if n.Digit != nil {
		index := *n.Digit
		return md.GetTypeByIndex(int(index))

	} else if n.Str != nil {
		return md.GetTypeByName(*n.Str)
	}
	return gtype.UNKNOWNTYPE, fmt.Errorf("wrong IdentifierNode")
}

func (n *IdentifierNode) GetColumns() ([]string, error) {
	if n.Digit != nil {
		return []string{}, nil
	} else if n.Str != nil {
		return []string{*n.Str}, nil
	}
	return []string{}, fmt.Errorf("wrong IdentifierNode")
}

func (n *IdentifierNode) Init(md *metadata.Metadata) error {
	if n.Str != nil {
		index, err := md.GetIndexByName(*n.Str)
		if err != nil {
			return err
		}
		n.Digit = &index
	}
	return nil
}

func (n *IdentifierNode) Result(input *row.RowsGroup) (interface{}, error) {
	rn := input.GetRowsNumber()
	index := 0

	if n.Digit != nil {
		if *n.Digit >= input.GetColumnsNumber() {
			return nil, fmt.Errorf("index out of range")
		}
		index = *n.Digit

	} else if n.Str != nil {
		index = input.GetColumnIndex(*n.Str)
		if index >= input.GetColumnsNumber() {
			return nil, fmt.Errorf("index out of range")
		}
	} else {
		return nil, fmt.Errorf("wrong IdentifierNode")
	}

	res := make([]interface{}, rn)
	for i := 0; i < rn; i++ {
		res[i] = input.Vals[index][i]
	}
	return res, nil
}

func (n *IdentifierNode) GetText() string {
	if n.Str != nil {
		return *n.Str
	} else if n.Digit != nil {
		return fmt.Sprintf("%d", *n.Digit)
	}
	return ""
}
