package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/pool"
)

// Nodeの種類
// 葉ノードと枝ノードがある
// どちらも8 bytes
const (
	LeafNodeType   = "LEAF    "
	BranchNodeType = "BRANCH  "
)

// バイトをdisk.PageIDに変換
// 主にMetaやLeafのIDを取得するときに使う
func toPageID(b []byte) disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(b))
}

func to8Bytes(i disk.PageID) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func to2Bytes(i uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, i)
	return b
}

// ======================================================================

type HeaderNode struct {
	nodeType []byte // 8 bytes
}

type Node struct {
	header HeaderNode
	body   []byte
}

func NewNode(page *pool.Page) (*Node, error) {
	pageData := page.GetData()

	if len(pageData) != disk.PageSize {
		return nil, fmt.Errorf("invalid page size: got %d, want %d", len(pageData), disk.PageSize)
	}

	node := &Node{
		header: HeaderNode{nodeType: pageData[:8]},
		body:   pageData[8:],
	}

	return node, nil
}

func (n *Node) SetNodeType(nodeType string) error {
	if nodeType != LeafNodeType && nodeType != BranchNodeType {
		return fmt.Errorf("invalid node type: %s", nodeType)
	}

	if len(nodeType) != 8 {
		return fmt.Errorf("invalid node type: %s", nodeType)
	}

	copy(n.header.nodeType, nodeType)

	return nil
}

func (n *Node) GetNodeType() string {
	return string(n.header.nodeType)
}

// ======================================================================

type MetaHeader struct {
	rootID []byte // 8 bytes, disk.PageID
}

type Meta struct {
	header MetaHeader
}

func NewMeta(page *pool.Page) (*Meta, error) {
	pageData := page.GetData()

	if len(pageData) != disk.PageSize {
		return nil, fmt.Errorf("invalid page size: got %d, want %d", len(pageData), disk.PageSize)
	}

	meta := &Meta{
		header: MetaHeader{rootID: pageData[:8]},
	}

	return meta, nil
}

func (m *Meta) GetID() disk.PageID {
	return toPageID(m.header.rootID)
}

func (m *Meta) SetID(pageID disk.PageID) error {
	if pageID <= disk.InvalidID {
		return fmt.Errorf("invalid page id: got %d", pageID)
	}
	copy(m.header.rootID, to8Bytes(pageID))
	return nil
}

// ======================================================================

type SlotHeader struct {
	numSlot   []byte
	freeSpace []byte
}

// 4072 bytes (Leafのbodyのサイズ)
//
//	header: 4 bytes
//	body: 4068 bytes
type Slot struct {
	header SlotHeader
	body   []byte
}

func (s *Slot) reset() {
	copy(s.header.numSlot, to2Bytes(0))
	copy(s.header.freeSpace, to2Bytes(uint16(len(s.body))))
}

// ======================================================================

type LeafHeader struct {
	prevID []byte // 8 bytes
	nextID []byte // 8 bytes
}

// 4088 bytes (Nodeのbodyのサイズ)
//
//	header: 16 bytes
//	body: 4072 bytes
type Leaf struct {
	header LeafHeader
	body   Slot
}

func NewLeaf(node *Node) (*Leaf, error) {
	nodeBody := node.body

	// ノードのヘッダーは8バイト、それは無視
	if len(nodeBody) != disk.PageSize-8 {
		return nil, fmt.Errorf("invalid page size: got %d, want %d", len(nodeBody), disk.PageSize-8)
	}

	leaf := &Leaf{
		// 16 bytes
		header: LeafHeader{
			prevID: nodeBody[:8],   // 8 bytes
			nextID: nodeBody[8:16], // 8 bytes
		},
		// 4072 bytes
		body: Slot{
			header: SlotHeader{
				numSlot:   nodeBody[16:18], // 2 bytes
				freeSpace: nodeBody[18:20], // 2 bytes
			},
			body: nodeBody[20:],
		},
	}

	return leaf, nil
}

func (l *Leaf) reset() {
	copy(l.header.prevID, to8Bytes(disk.InvalidID))
	copy(l.header.nextID, to8Bytes(disk.InvalidID))
	l.body.reset()
}

func (l *Leaf) GetPrevID() disk.PageID {
	return toPageID(l.header.prevID)
}

func (l *Leaf) GetNextID() disk.PageID {
	return toPageID(l.header.nextID)
}

func (l *Leaf) GetNumSlots() uint16 {
	return binary.LittleEndian.Uint16(l.body.header.numSlot)
}

func (l *Leaf) GetFreeSpace() uint16 {
	return binary.LittleEndian.Uint16(l.body.header.freeSpace)
}

// ======================================================================

// type BranchHeader struct {
// 	rightID disk.PageID
// }

// type Branch struct {
// 	header BranchHeader
// 	body   Slot
// }

// ======================================================================

type BTree struct {
	metaID disk.PageID
}

// 生成される[metaPage]と[rootPage]は、btreeが存在する限り、常に存在する（unpinされない）
func NewBTree(poolManager *pool.PoolManager) (*BTree, error) {
	// メタデータ作成
	metaID, err := poolManager.CreatePage()
	if err != nil {
		return nil, err
	}
	metaPage, err := poolManager.FetchPage(metaID)
	if err != nil {
		return nil, err
	}

	metaData, err := NewMeta(metaPage)
	if err != nil {
		return nil, err
	}

	// ルートID作成
	rootID, err := poolManager.CreatePage()
	if err != nil {
		return nil, err
	}

	// メタデータにルートIDをセット
	err = metaData.SetID(rootID)
	if err != nil {
		return nil, err
	}

	// ルートページ取得
	rootPage, err := poolManager.FetchPage(rootID)
	if err != nil {
		return nil, err
	}

	// ルートノード作成
	rootNode, err := NewNode(rootPage)
	if err != nil {
		return nil, err
	}
	// ルートノードのノードタイプをセット
	err = rootNode.SetNodeType(LeafNodeType)
	if err != nil {
		return nil, err
	}

	// リーフノード作成
	leaf, err := NewLeaf(rootNode)
	if err != nil {
		return nil, err
	}
	// リーフノードを初期化
	leaf.reset()

	return &BTree{
		metaID: metaID,
	}, nil
}

func (b *BTree) GetMetaID() disk.PageID {
	return b.metaID
}

func (b *BTree) Clear(poolManager *pool.PoolManager) error {
	metaPage, err := poolManager.FetchPage(b.metaID)
	if err != nil {
		return err
	}
	// ここで作成したページとBtree作成時に作ったページをアンピン
	defer metaPage.Unpin()
	defer metaPage.Unpin()

	metaData, err := NewMeta(metaPage)
	if err != nil {
		return err
	}

	rootPage, err := poolManager.FetchPage(metaData.GetID())
	if err != nil {
		return err
	}
	// ここで作成したページとBtree作成時に作ったページをアンピン
	defer rootPage.Unpin()
	defer rootPage.Unpin()

	b.metaID = disk.InvalidID

	return nil
}
