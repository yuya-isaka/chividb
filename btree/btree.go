package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/pool"
)

// ======================================================================

/*

Node

	nodeType (Leaf or Branch or Meta) 						... 8 bytes
	NodeBody (Leaf or Branch or Meta) 			 			... 4088 bytes

		↓																							↓

		If Meta
			rootID										 									... 8 bytes

		If Leaf
			prevID 																			... 8 bytes
			nextID 																			... 8 bytes
			LeafBody (Slot) 														... 4072 bytes
				↓																						↓
				numSlot 																		... 2 bytes
				numFree 																		... 2 bytes
				SlotBody																		... 4068 bytes

		If Branch
			rightID 																		... 8 bytes
			BranchBody (Slot) 							 						... 4080 bytes
				↓																						↓
				numSlot 																		... 2 bytes
				numFree 																		... 2 bytes
				SlotBody																		... 4076 bytes

*/

// ======================================================================

// nt と string を区別する
type NodeType interface {
	xxxProtexted()
}

type nt string

func (n nt) xxxProtexted() {}

const (
	MetaNodeType   nt = "META    " // メタノード、8 bytes
	LeafNodeType   nt = "LEAF    " // 葉ノード、8 bytes
	BranchNodeType nt = "BRANCH  " // 枝ノード、8 bytes
)

// ======================================================================

// 8byteバイトスライス → disk.PageID
func toPageID(b []byte) disk.PageID {
	if len(b) != 8 {
		return disk.InvalidPageID
	}
	return disk.PageID(binary.LittleEndian.Uint64(b)) // 符号なし64ビット整数
}

// disk.PageID → 8byteバイトスライス
func to8Bytes(i disk.PageID) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i)) // バイトスライス
	return b
}

// uint16 → 2byteバイトスライス
func to2Bytes(i uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, i) // バイトスライス
	return b
}

// ======================================================================

type Node struct {
	nodeType []byte // 8 bytes, MetaNodeTyoe or LeafNodeType or BranchNodeType
	nodeBody []byte // 4088 bytes
}

func NewNode(page *pool.Page) (*Node, error) {
	pageData := page.GetPageData() // 4096 bytes
	if len(pageData) != disk.PageSize {
		return nil, fmt.Errorf("invalid page size: got %d, want %d", len(pageData), disk.PageSize)
	}

	node := &Node{
		nodeType: pageData[:8], // 8 bytes
		nodeBody: pageData[8:], // 4088 bytes
	}

	return node, nil
}

func (n *Node) getNodeType() NodeType {
	return nt(n.nodeType)
}

func (n *Node) setNodeType(nodeType NodeType) {
	if tmp, ok := nodeType.(nt); ok {
		copy(n.nodeType, tmp)
	}
}

// ======================================================================

// BTreeのルートIDを保持する
type Meta struct {
	rootID []byte // 8 bytes, disk.PageID
}

func NewMeta(node *Node) (*Meta, error) {
	if nt(node.nodeType) != MetaNodeType {
		return nil, fmt.Errorf("invalid node type: got %s, want %s", node.nodeType, MetaNodeType)
	}

	meta := &Meta{
		rootID: node.nodeBody[:8], // 8 bytes
	}

	return meta, nil
}

// func (m *Meta) getRootID() disk.PageID {
// 	return toPageID(m.rootID)
// }

func (m *Meta) setRootID(rootID disk.PageID) error {
	if rootID <= disk.InvalidPageID {
		return fmt.Errorf("invalid page id: got %d", rootID)
	}

	copy(m.rootID, to8Bytes(rootID))

	return nil
}

// ======================================================================

type Slot struct {
	slotNum  []byte // 2 bytes, uint16
	slotFree []byte // 2 bytes, uint16
	slotBody []byte // 4068 bytes (Leaf) or 4076 bytes (Branch)
}

func (s *Slot) reset() {
	copy(s.slotNum, to2Bytes(0))
	copy(s.slotFree, to2Bytes(uint16(len(s.slotBody))))
}

// ======================================================================

type Leaf struct {
	prevID   []byte // 8 bytes
	nextID   []byte // 8 bytes
	leafBody Slot   // 4072 bytes
}

func NewLeaf(node *Node) (*Leaf, error) {
	if nt(node.nodeType) != LeafNodeType {
		return nil, fmt.Errorf("invalid node type: got %s, want %s", node.nodeType, LeafNodeType)
	}

	leaf := &Leaf{
		prevID: node.nodeBody[:8],   // 8 bytes
		nextID: node.nodeBody[8:16], // 8 bytes
		leafBody: Slot{
			slotNum:  node.nodeBody[16:18], // 2 bytes
			slotFree: node.nodeBody[18:20], // 2 bytes
			slotBody: node.nodeBody[20:],   // 4068 bytes
		},
	}

	return leaf, nil
}

func (l *Leaf) reset() {
	copy(l.prevID, to8Bytes(disk.InvalidPageID))
	copy(l.nextID, to8Bytes(disk.InvalidPageID))

	l.leafBody.reset()
}

func (l *Leaf) GetPrevID() disk.PageID {
	return toPageID(l.prevID)
}

func (l *Leaf) GetNextID() disk.PageID {
	return toPageID(l.nextID)
}

func (l *Leaf) GetNumSlots() uint16 {
	return binary.LittleEndian.Uint16(l.leafBody.slotNum)
}

func (l *Leaf) GetFreeSpace() uint16 {
	return binary.LittleEndian.Uint16(l.leafBody.slotFree)
}

// ======================================================================

type BTree struct {
	metaID disk.PageID
}

// 生成される[metaPage]と[rootPage]は、btreeが存在する限り、常に存在する（unpinされない）
func NewBTree(poolManager *pool.PoolManager) (*BTree, disk.PageID, error) {
	// メタページ作成
	metaID, err := poolManager.CreatePage()
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	// ルートページ作成
	rootID, err := poolManager.CreatePage()
	if err != nil {
		return nil, disk.InvalidPageID, err
	}

	// メタページ取得
	metaPage, err := poolManager.FetchPage(metaID)
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	// メタノード作成と初期化
	metaNode, err := NewNode(metaPage)
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	metaPage.SetUpdateFlag(true)
	metaNode.setNodeType(MetaNodeType)
	// メタデータ取得と初期化
	metaData, err := NewMeta(metaNode)
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	if err = metaData.setRootID(rootID); err != nil {
		return nil, disk.InvalidPageID, err
	}

	// メタページをアンピン
	metaPage.Unpin()

	// ルートページ取得
	rootPage, err := poolManager.FetchPage(rootID)
	if err != nil {
		return nil, disk.InvalidPageID, err
	}

	// ルートノード作成と初期化
	rootNode, err := NewNode(rootPage)
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	rootPage.SetUpdateFlag(true)
	rootNode.setNodeType(LeafNodeType)

	// ルートノードからリーフノード取得と初期化
	leaf, err := NewLeaf(rootNode)
	if err != nil {
		return nil, disk.InvalidPageID, err
	}
	leaf.reset()

	// メタページとルートページをアンピン
	rootPage.Unpin()

	return &BTree{
		metaID: metaID, // メタデータのページIDはここでセットするから、SetMetaID()は不要
	}, rootID, nil
}

func (b *BTree) GetMetaID() disk.PageID {
	return b.metaID
}
