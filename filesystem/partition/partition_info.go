package partition

import (
	"github.com/gotodb/gotodb/filesystem"
	"github.com/gotodb/gotodb/metadata"
	"github.com/gotodb/gotodb/row"
)

type PartitionInfo struct {
	Metadata   *metadata.Metadata
	Partitions []*Partition
	Locations  []string
	FileTypes  []filesystem.FileType
	FileLists  [][]*filesystem.FileLocation

	//for no partition
	FileList []*filesystem.FileLocation
}

func NewPartitionInfo(md *metadata.Metadata) *PartitionInfo {
	res := &PartitionInfo{
		Metadata:  md,
		Locations: []string{},
		FileTypes: []filesystem.FileType{},
		FileLists: [][]*filesystem.FileLocation{},

		FileList: []*filesystem.FileLocation{},
	}
	for i := 0; i < md.GetColumnNumber(); i++ {
		t, _ := md.GetTypeByIndex(i)
		par := NewPartition(t)
		res.Partitions = append(res.Partitions, par)
	}
	return res
}

func (p *PartitionInfo) GetPartitionColumnNum() int {
	return len(p.Partitions)
}

func (p *PartitionInfo) GetPartitionNum() int {
	if len(p.Partitions) <= 0 {
		return 0
	}
	return len(p.Partitions[0].Vals)
}

func (p *PartitionInfo) GetPartitionRowGroup(i int) *row.RowsGroup {
	r := p.GetPartitionRow(i)
	if r == nil {
		return nil
	}
	rb := row.NewRowsGroup(p.Metadata)
	rb.Write(r)
	return rb
}

func (p *PartitionInfo) GetPartitionRow(i int) *row.Row {
	if i >= p.GetPartitionNum() {
		return nil
	}
	r := new(row.Row)
	for j := 0; j < len(p.Partitions); j++ {
		r.AppendVals(p.Partitions[j].Vals[i])
	}
	return r
}

func (p *PartitionInfo) GetPartitionFiles(i int) []*filesystem.FileLocation {
	if i >= len(p.FileLists) {
		return []*filesystem.FileLocation{}
	}
	return p.FileLists[i]
}

func (p *PartitionInfo) GetNoPartititonFiles() []*filesystem.FileLocation {
	return p.FileList
}

func (p *PartitionInfo) GetLocation(i int) string {
	if i >= len(p.Locations) {
		return ""
	}
	return p.Locations[i]
}

func (p *PartitionInfo) GetFileType(i int) filesystem.FileType {
	if i >= len(p.FileTypes) {
		return filesystem.UNKNOWNFILETYPE
	}
	return p.FileTypes[i]
}

func (p *PartitionInfo) Write(row *row.Row) {
	for i, val := range row.Vals {
		p.Partitions[i].Append(val)
	}
}

func (p *PartitionInfo) IsPartition() bool {
	if p.Metadata != nil && len(p.Metadata.Columns) > 0 {
		return true
	}
	return false
}

func (p *PartitionInfo) Encode() {
	for _, par := range p.Partitions {
		par.Encode()
	}
}

func (p *PartitionInfo) Decode() error {
	for _, par := range p.Partitions {
		if err := par.Decode(); err != nil {
			return err
		}
	}
	return nil
}
