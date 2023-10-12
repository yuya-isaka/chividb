package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/yuya-isaka/chibidb/btree/leaf"
	"github.com/yuya-isaka/chibidb/btree/node"
	"github.com/yuya-isaka/chibidb/btree/util"
	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/pool"
)

// ======================================================================
/*

Leaf
nodeType   prevID  +   nextID   +   slotNum   +   slotFreeOffset   +  slotBody
	8byte    8 bytes    8 bytes    	2 bytes    		2 bytes 					4068 bytes

Branch
nodeType   rightID   +   slotNum   +   slotFreeOffset   +  slotBody
	8byte     8 bytes    	2 bytes    		2 bytes 					4076 bytes

Node (Page)
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
					↓
					Pointer 																		... 4 bytes
		If Branch
			rightID 																		... 8 bytes
			BranchBody (Slot) 							 						... 4080 bytes
				↓																						↓
				numSlot 																		... 2 bytes
				numFree 																		... 2 bytes
				SlotBody																		... 4076 bytes
					↓
					Pointer 																		... 4 bytes
*/
// ======================================================================

// BTreeのルートIDを保持する
type Meta struct {
	metaUpdate *bool
	rootID     []byte // 8 bytes, disk.PageID
}

func NewMeta(n *node.Node) (*Meta, error) {
	if n.GetNodeType() != node.MetaNodeType {
		return nil, fmt.Errorf("invalid node type: got %s, want %s", n.GetNodeType(), node.MetaNodeType)
	}

	return &Meta{
		metaUpdate: n.GetRefupdateFlag(),
		rootID:     n.GetRefnodeBody()[:8], // 8 bytes
	}, nil
}

// Get関係 ======================================================================

func (m *Meta) getRootID() disk.PageID {
	// setの段階で、rootIDがdisk.InvalidPageIDの場合は、エラーを返すようにしているので、ここでエラーチェックは不要
	return disk.PageID(binary.LittleEndian.Uint64(m.rootID))
}

// Set関係 ======================================================================

func (m *Meta) setRootID(rootID disk.PageID) error {
	if rootID <= disk.InvalidPageID {
		return fmt.Errorf("invalid page id: got %d", rootID)
	}
	*m.metaUpdate = true
	copy(m.rootID, util.PageIDToBytes(rootID))
	return nil
}

// ======================================================================

type BTree struct {
	metaID      disk.PageID
	poolManager *pool.PoolManager
}

// 生成される[metaPage]と[rootPage]は、btreeが存在する限り、常に存在する（unpinされない）
func NewBTree(poolManager *pool.PoolManager) (*BTree, error) {
	// メタページ作成
	metaID, err := poolManager.CreatePage()
	if err != nil {
		return nil, err
	}
	// ルートページ作成
	rootID, err := poolManager.CreatePage()
	if err != nil {
		return nil, err
	}

	// メタページ取得
	metaPage, err := poolManager.FetchPage(metaID)
	if err != nil {
		return nil, err
	}
	// メタノード作成と初期化
	metaNode, err := node.NewNode(metaPage)
	if err != nil {
		return nil, err
	}
	metaNode.SetNodeType(node.MetaNodeType)
	// メタデータ取得と初期化
	metaData, err := NewMeta(metaNode)
	if err != nil {
		return nil, err
	}
	if err = metaData.setRootID(rootID); err != nil {
		return nil, err
	}

	// メタページをアンピン
	metaPage.Unpin()

	// ルートページ取得
	rootPage, err := poolManager.FetchPage(rootID)
	if err != nil {
		return nil, err
	}

	// ルートノード作成と初期化
	rootNode, err := node.NewNode(rootPage)
	if err != nil {
		return nil, err
	}
	rootNode.SetNodeType(node.LeafNodeType)

	// ルートノードからリーフノード取得と初期化
	leaf, err := leaf.NewLeaf(rootNode)
	if err != nil {
		return nil, err
	}
	leaf.ResetLeaf()

	// メタページとルートページをアンピン
	rootPage.Unpin()

	return &BTree{
		metaID:      metaID, // メタデータのページIDはここでセットするから、SetMetaID()は不要
		poolManager: poolManager,
	}, nil
}

// Get関係 ======================================================================

func (b *BTree) GetMetaID() disk.PageID {
	return b.metaID
}

// Set関係 ======================================================================

func (b *BTree) set(page *pool.Page, key []byte, value []byte) ([]byte, disk.PageID, error) {
	n, err := node.NewNode(page)
	if err != nil {
		return nil, disk.InvalidPageID, err
	}

	switch n.GetNodeType() {
	case node.LeafNodeType:
		l, err := leaf.NewLeaf(n)
		if err != nil {
			return nil, disk.InvalidPageID, err
		}
		slotID, ok := l.SearchSlotID(key)
		if ok {
			return nil, disk.InvalidPageID, fmt.Errorf("duplicate key: %s", key)
		}
		// TODO
		fmt.Println(slotID)
	case node.BranchNodeType:
		// TODO
	}

	return nil, disk.InvalidPageID, fmt.Errorf("invalid node type")
}

func (b *BTree) Insert(key []byte, value []byte) error {
	metaPage, err := b.poolManager.FetchPage(b.metaID)
	if err != nil {
		return err
	}
	metaNode, err := node.NewNode(metaPage)
	if err != nil {
		return err
	}
	metaData, err := NewMeta(metaNode)
	if err != nil {
		return err
	}

	rootPage, err := b.poolManager.FetchPage(metaData.getRootID())
	if err != nil {
		return err
	}

	newKey, childPageID, err := b.set(rootPage, key, value)
	if err != nil {
		return err
	}
	// TODO
	fmt.Println(newKey, childPageID)

	return nil
}
