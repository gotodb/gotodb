package util

import (
	"container/heap"

	"github.com/gotodb/gotodb/pb"
	"github.com/satori/go.uuid"
)

type Item struct {
	Location       *pb.Location
	ExecutorNumber int
}

func NewItem(loc *pb.Location, num int) *Item {
	return &Item{
		Location:       loc,
		ExecutorNumber: num,
	}
}

type Heap struct {
	Items    []*Item
	AgentMap map[string]*pb.Location
}

func NewHeap() *Heap {
	return &Heap{
		Items:    []*Item{},
		AgentMap: map[string]*pb.Location{},
	}
}

func (h *Heap) Len() int { return len(h.Items) }
func (h *Heap) Less(i, j int) bool {
	return h.Items[i].ExecutorNumber < h.Items[j].ExecutorNumber
}
func (h *Heap) Swap(i, j int)         { h.Items[i], h.Items[j] = h.Items[j], h.Items[i] }
func (h *Heap) Push(item interface{}) { h.Items = append(h.Items, item.(*Item)) }
func (h *Heap) Pop() interface{} {
	n := len(h.Items)
	x := h.Items[n-1]
	h.Items = h.Items[:n-1]
	return x
}

func (h *Heap) HasExecutor() bool {
	if h.Len() > 0 {
		return true
	} else {
		return false
	}
}

func (h *Heap) GetExecutorLoc() *pb.Location {
	item := heap.Pop(h).(*Item)
	exe := &pb.Location{
		Name:    "executor_" + uuid.NewV4().String(),
		Address: item.Location.Address,
		Port:    item.Location.Port,
	}
	h.AgentMap[item.Location.Name] = item.Location

	item.ExecutorNumber++
	heap.Push(h, item)
	return exe
}
