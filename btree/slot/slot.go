package slot

import (
	"encoding/binary"
	"fmt"

	"github.com/yuya-isaka/chibidb/btree/node"
	"github.com/yuya-isaka/chibidb/btree/pair"
	"github.com/yuya-isaka/chibidb/btree/spointer"
	"github.com/yuya-isaka/chibidb/btree/util"
	"github.com/yuya-isaka/chibidb/disk"
)

type Slot struct {
	slotUpdate     *bool
	slotNum        []byte // 2 bytes, uint16
	slotFreeOffset []byte // 2 bytes, uint16
	slotBody       []byte // 4068 bytes (Leaf) or 4076 bytes (Branch)
}

func NewSlot(n *node.Node) (*Slot, error) {
	if len(n.GetRefnodeBody()) != 4088 {
		return nil, fmt.Errorf("invalid node body size: got %d, want %d", len(n.GetRefnodeBody()), 4088)
	}

	switch n.GetNodeType() {
	case node.LeafNodeType: // 葉ノード
		return &Slot{
			slotUpdate:     n.GetRefupdateFlag(),
			slotNum:        n.GetRefnodeBody()[16:18], // 2 bytes
			slotFreeOffset: n.GetRefnodeBody()[18:20], // 2 bytes
			slotBody:       n.GetRefnodeBody()[20:],   // 4068 bytes (Leaf) or 4076 bytes (Branch)
		}, nil
	case node.BranchNodeType: // 枝ノード
		return &Slot{
			slotUpdate:     n.GetRefupdateFlag(),
			slotNum:        n.GetRefnodeBody()[8:10],  // 2 bytes
			slotFreeOffset: n.GetRefnodeBody()[10:12], // 2 bytes
			slotBody:       n.GetRefnodeBody()[12:],   // 4068 bytes (Leaf) or 4076 bytes (Branch)
		}, nil
	}
	// unreachable
	return nil, fmt.Errorf("invalid node type: got %s, want %s or %s", n.GetNodeType(), node.LeafNodeType, node.BranchNodeType)
}

func (s *Slot) ResetSlot() {
	*s.slotUpdate = true

	copy(s.slotNum, util.Uint16ToBytes(0))
	copy(s.slotFreeOffset, util.Uint16ToBytes(uint16(disk.PageSize)))
}

// Get関係 ======================================================================

func (s *Slot) GetSlotNum() uint16 {
	return binary.LittleEndian.Uint16(s.slotNum)
}

func (s *Slot) GetFreeOffset() uint16 {
	return binary.LittleEndian.Uint16(s.slotFreeOffset)
}

func (s *Slot) GetFreeNum() uint16 {
	slotBodyOffset := uint16(disk.PageSize - len(s.slotBody))

	return s.GetFreeOffset() - slotBodyOffset - s.GetSlotNum()*4
}

func (s *Slot) GetPair(index uint16) *pair.Pair {
	return pair.NewPair(s.slotBody, spointer.NewSPointer(s.slotBody, index))
}

// Set関係 ======================================================================

// 指定したスロットインデックスにペアをセットする
func (s *Slot) Set(index uint16, pair *pair.Pair) error {
	// keyLengthを格納する2バイトも含める
	pairSize := pair.GetSize() + 2
	// ポインタのサイズも含める
	if s.GetFreeNum() < pairSize+4 {
		return fmt.Errorf("no free space: got %d, want %d", s.GetFreeNum(), pairSize)
	}

	// 1. スロット数とフリーオフセット更新
	copy(s.slotNum, util.Uint16ToBytes(s.GetSlotNum()+1))
	copy(s.slotFreeOffset, util.Uint16ToBytes(s.GetFreeOffset()-pairSize))

	// 2. スロットポインタ更新
	// offset
	copy(s.slotBody[index*4:index*4+2], util.Uint16ToBytes(s.GetFreeOffset()))
	// length
	copy(s.slotBody[index*4+2:index*4+4], util.Uint16ToBytes(pairSize))

	// 3. スロットボディ更新
	// keyLength 2byte
	copy(s.slotBody[s.GetFreeOffset():s.GetFreeOffset()+2], util.Uint16ToBytes(pair.GetKeySize()))
	// key
	copy(s.slotBody[s.GetFreeOffset()+2:s.GetFreeOffset()+2+pair.GetKeySize()], pair.GetKey())
	// value
	copy(s.slotBody[s.GetFreeOffset()+2+pair.GetKeySize():s.GetFreeOffset()+pairSize], pair.GetValue())

	return nil
}
