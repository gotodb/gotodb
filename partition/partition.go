package partition

import (
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type Partition struct {
	Metadata  *metadata.Metadata
	Parts     []*Part
	Locations []string
	FileTypes []FileType
	FileLists [][]*FileLocation
}

func New(md *metadata.Metadata) *Partition {
	res := &Partition{
		Metadata:  md,
		Locations: []string{},
		FileTypes: []FileType{},
		FileLists: [][]*FileLocation{},
	}
	for i := 0; i < md.GetColumnNumber(); i++ {
		t, _ := md.GetTypeByIndex(i)
		par := NewPartition(t)
		res.Parts = append(res.Parts, par)
	}
	return res
}

func (p *Partition) GetPartitionColumnNum() int {
	return len(p.Parts)
}

func (p *Partition) GetPartitionNum() int {
	if len(p.Parts) <= 0 {
		return 0
	}
	return len(p.Parts[0].Vals)
}

func (p *Partition) GetPartitionRowGroup(i int) *row.RowsGroup {
	r := p.GetPartitionRow(i)
	if r == nil {
		return nil
	}
	rb := row.NewRowsGroup(p.Metadata)
	rb.Write(r)
	return rb
}

func (p *Partition) GetPartitionRow(i int) *row.Row {
	if i >= p.GetPartitionNum() {
		return nil
	}
	r := new(row.Row)
	for j := 0; j < len(p.Parts); j++ {
		r.AppendVals(p.Parts[j].Vals[i])
	}
	return r
}

func (p *Partition) GetPartitionFiles(i int) []*FileLocation {
	if i >= len(p.FileLists) {
		return []*FileLocation{}
	}
	return p.FileLists[i]
}

func (p *Partition) GetNoPartitionFiles() []*FileLocation {
	var f []*FileLocation
	for i, location := range p.Locations {
		f = append(f, NewFileLocation(location, p.GetFileType(i)))
	}
	return f
}

func (p *Partition) GetLocation(i int) string {
	if i >= len(p.Locations) {
		return ""
	}
	return p.Locations[i]
}

func (p *Partition) GetFileType(i int) FileType {
	if i >= len(p.FileTypes) {
		return FileTypeUnknown
	}
	return p.FileTypes[i]
}

func (p *Partition) Write(row *row.Row) {
	for i, val := range row.Vals {
		p.Parts[i].Append(val)
	}
}

func (p *Partition) IsPartition() bool {
	if p.Metadata != nil && len(p.Metadata.Columns) > 0 {
		return true
	}
	return false
}

func (p *Partition) Encode() {
	for _, par := range p.Parts {
		par.Encode()
	}
}

func (p *Partition) Decode() error {
	for _, par := range p.Parts {
		if err := par.Decode(); err != nil {
			return err
		}
	}
	return nil
}
