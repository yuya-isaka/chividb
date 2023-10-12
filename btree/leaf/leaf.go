package leaf

import (
	"encoding/binary"
	"fmt"

	"github.com/yuya-isaka/chibidb/btree/bsearch"
	"github.com/yuya-isaka/chibidb/btree/node"
	"github.com/yuya-isaka/chibidb/btree/pair"
	"github.com/yuya-isaka/chibidb/btree/slot"
	"github.com/yuya-isaka/chibidb/btree/util"
	"github.com/yuya-isaka/chibidb/disk"
)

type Leaf struct {
	leafUpdate *bool
	prevID     []byte    // 8 bytes
	nextID     []byte    // 8 bytes
	leafSlot   slot.Slot // 4072 bytes
}

func NewLeaf(n *node.Node) (*Leaf, error) {
	if n.GetNodeType() != node.LeafNodeType {
		return nil, fmt.Errorf("invalid node type: got %s, want %s", n.GetNodeType(), node.LeafNodeType)
	}
	// スロット取得
	slot, err := slot.NewSlot(n)
	if err != nil {
		return nil, err
	}
	return &Leaf{
		leafUpdate: n.GetRefupdateFlag(),
		prevID:     n.GetRefnodeBody()[:8],   // 8 bytes
		nextID:     n.GetRefnodeBody()[8:16], // 8 bytes
		leafSlot:   *slot,
	}, nil
}

func (l *Leaf) ResetLeaf() {
	*l.leafUpdate = true

	copy(l.prevID, util.PageIDToBytes(disk.InvalidPageID))
	copy(l.nextID, util.PageIDToBytes(disk.InvalidPageID))

	l.leafSlot.ResetSlot()
}

// Get関係 ======================================================================

func (l *Leaf) GetPrevID() disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(l.prevID))
}

func (l *Leaf) GetNextID() disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(l.nextID))
}

func (l *Leaf) GetSlotNum() uint16 {
	return l.leafSlot.GetSlotNum()
}

func (l *Leaf) GetFreeNum() uint16 {
	return l.leafSlot.GetFreeNum()
}

func (l *Leaf) getLimitPairSize() uint16 {
	return l.GetFreeNum() / 2
}

// Set関係 ======================================================================

func (l *Leaf) Insert(slotID uint16, p *pair.Pair) error {
	if p.GetSize() > l.getLimitPairSize() {
		return fmt.Errorf("invalid pair size: got %d, want %d", p.GetSize(), l.getLimitPairSize())
	}

	if err := l.leafSlot.Set(slotID, p); err != nil {
		return err
	}

	return nil
}

// Search関係 ======================================================================

// slotID と 見つかったかのフラグを返す
func (l *Leaf) SearchSlotID(key []byte) (uint16, bool) {
	return bsearch.BinarySearch(l.GetSlotNum(), func(i uint16) bsearch.Ordering {
		targetKey := l.leafSlot.GetPair(i).GetKey()
		return util.CompareByteSlice(targetKey, key)
	})
}
