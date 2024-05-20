package btree

import (
	"errors"
	"log"

	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/page"
	"github.com/yuya-isaka/chibidb/pool"
	"github.com/yuya-isaka/chibidb/util"
)

type BTree struct {
	rootID      disk.PageID
	poolManager *pool.PoolManager
}

func NewBTree(poolManager *pool.PoolManager) (*BTree, error) {
	// rootID == 0
	rootID, err := poolManager.CreatePage()
	if err != nil {
		return nil, err
	}

	// rootPage
	rootPage, err := poolManager.FetchPage(rootID)
	if err != nil {
		return nil, err
	}
	// リーフノードの設定だけでいいはず
	// rootPageは一番最初はリーフノード
	rootPage.SetNodeType(page.LeafNodeType)

	return &BTree{
		rootID:      rootID,
		poolManager: poolManager,
	}, nil
}

func (b *BTree) Search(key []byte) ([]byte, error) {
	current, err := b.poolManager.FetchPage(b.rootID)
	if err != nil {
		return nil, err
	}

	for current != nil {
		var i uint16 = 0
		for i < current.GetPointersNum() && util.CompareByteSlice(key, current.GetKey(i)) == util.Greater {
			i++
		}

		if i < current.GetPointersNum() && util.CompareByteSlice(key, current.GetKey(i)) == util.Equal {
			if current.GetNodeType() == page.LeafNodeType {
				return current.GetValue(i), nil
			}
		}

		if current.GetNodeType() == page.LeafNodeType {
			return nil, errors.New("key not found")
		}

		newPageId := util.BytesToPageID(current.GetValue(i))
		current, err = b.poolManager.FetchPage(newPageId)
		if err != nil {
			return nil, err
		}
	}

	return nil, errors.New("key not found")
}

func (b *BTree) Insert(key []byte, value []byte) error {
	rootPage, err := b.poolManager.FetchPage(b.rootID)
	if err != nil {
		return err
	}
	if rootPage.GetPointersNum() == 0 {
		rootPage.InsertPair(0, page.NewPair(key, value))
		return nil
	}

	// freeNumがpage.MaxPairSizeの半分よりも小さくなったら分割
	if rootPage.GetFreeNum()*2 < page.MaxPairSize {
		// 新しいrootPageを作成
		newRootID, err := b.poolManager.CreatePage()
		if err != nil {
			return err
		}
		newRootPage, err := b.poolManager.FetchPage(newRootID)
		if err != nil {
			return err
		}
		newRootPage.SetNodeType(page.BranchNodeType)
		// Keyは現時点では不明
		// ValueはrootID
		newRootPage.InsertPair(0, page.NewPair(nil, util.PageIDTo8Bytes(b.rootID)))
		b.splitChild(newRootPage, 0)
		b.rootID = newRootID
	}

	b.insertNonFull(rootPage, key, value)

	return nil
}

// ここら辺バグってる
func (b *BTree) insertNonFull(nodePage *page.Page, key []byte, value []byte) {
	idx := nodePage.GetPointersNum() - 1
	for idx != 0 && util.CompareByteSlice(nodePage.GetKey(idx), key) == util.Greater {
		idx--
	}
	idx++

	switch nodePage.GetNodeType() {
	case page.LeafNodeType:
		// キーと値を挿入
		nodePage.InsertPair(idx, page.NewPair(key, value))
		return
	case page.BranchNodeType:
		if nodePage.GetFreeNum()*2 < page.MaxPairSize {
			b.splitChild(nodePage, idx)
			if util.CompareByteSlice(key, nodePage.GetKey(idx)) == util.Greater {
				idx++
			}
		}
		targetPageID := nodePage.GetValue(idx)
		targetPage, err := b.poolManager.FetchPage(util.BytesToPageID(targetPageID))
		if err != nil {
			log.Panicf("failed to fetch page: %v", err)
			return
		}
		b.insertNonFull(targetPage, key, value)
	}
}

func (b *BTree) splitChild(parentPage *page.Page, idx uint16) {
	oldPageID := util.BytesToPageID(parentPage.GetValue(idx))
	oldPage, err := b.poolManager.FetchPage(oldPageID)
	if err != nil {
		log.Panicf("failed to fetch page: %v", err)
		return
	}

	// 新しいページを作成
	newPageID, err := b.poolManager.CreatePage()
	if err != nil {
		log.Panicf("failed to create page: %v", err)
		return
	}
	newPage, err := b.poolManager.FetchPage(newPageID)
	if err != nil {
		log.Panicf("failed to fetch page: %v", err)
		return
	}

	// childPageの中央のペア以降をnewPageに移動
	medianIdx := oldPage.GetPointersNum() / 2
	for i := medianIdx + 1; i < oldPage.GetPointersNum(); i++ {
		newPage.InsertPair(uint16(i-medianIdx-1), page.NewPair(oldPage.GetKey(i), oldPage.GetValue(i)))
	}
	medianKey := oldPage.GetKey(medianIdx)

	switch oldPage.GetNodeType() {
	case page.LeafNodeType:
		newPage.SetNodeType(page.LeafNodeType)
		oldPage.DeletePair(medianIdx + 1)
	case page.BranchNodeType:
		newPage.SetNodeType(page.BranchNodeType)
		for i := medianIdx + 1; i < oldPage.GetPointersNum(); i++ {
			newPage.InsertPair(uint16(i-medianIdx-1), oldPage.GetPair(i))
		}
		oldPage.DeletePair(medianIdx + 1)
	}

	// parentPageにnewPageの最初のキーを挿入
	parentPage.InsertPair(idx, page.NewPair(medianKey, util.PageIDTo8Bytes(newPageID)))
}
