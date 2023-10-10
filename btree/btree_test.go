package btree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/pool"
)

// func TestMain(m *testing.M) {
// 	goleak.VerifyTestMain(m)
// }

func TestBTree_InsertAndSearch(t *testing.T) {

	// 準備
	assert := assert.New(t)

	t.Run("Create BTree", func(t *testing.T) {

		// ファイルマネージャ
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// プール
		testPool := pool.NewPool(10)

		// プールマネージャ
		poolManager := pool.NewPoolManager(fileManager, testPool)
		defer poolManager.Close()

		// BTree
		tree, err := NewBTree(poolManager)
		assert.NoError(err)

		assert.Equal(disk.PageID(0), tree.GetMetaID())

		// ======================================================================

		// メタチェック
		metaPage, err := poolManager.FetchPage(tree.GetMetaID())
		assert.NoError(err)
		metaNode, err := NewNode(metaPage)
		assert.NoError(err)
		metaData, err := NewMeta(metaNode)
		assert.NoError(err)

		assert.Equal(MetaNodeType, metaNode.getNodeType())
		assert.Equal(disk.PageID(1), metaData.getRootID())

		// ======================================================================

		// ルートチェック
		rootPage, err := poolManager.FetchPage(metaData.getRootID())
		assert.NoError(err)
		rootNode, err := NewNode(rootPage)
		assert.NoError(err)

		assert.Equal(disk.PageID(1), rootPage.GetPageID())
		assert.Equal(LeafNodeType, rootNode.getNodeType())

		// ======================================================================

		// リーフチェック
		leaf, err := NewLeaf(rootNode)
		assert.NoError(err)

		assert.Equal(disk.InvalidPageID, leaf.GetPrevID())
		assert.Equal(disk.InvalidPageID, leaf.GetNextID())
		assert.Equal(uint16(0), leaf.GetNumSlot())
		assert.Equal(uint16(4068), leaf.GetNumFree())

		// ======================================================================

		// テストで作成したページをアンピン
		metaPage.Unpin()
		rootPage.Unpin()

		assert.Equal(pool.Pin(-1), metaPage.GetPinCount())
		assert.Equal(pool.Pin(-1), rootPage.GetPinCount())
	})

	// ======================================================================
	// ======================================================================

	t.Run("Create Read BTree with Pool 1", func(t *testing.T) {

		// ファイルマネージャ
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// ======================================================================

		// プール
		testPool := pool.NewPool(1)

		// プールマネージャ
		poolManager := pool.NewPoolManager(fileManager, testPool)
		defer poolManager.Close()

		// BTree
		tree, err := NewBTree(poolManager)
		assert.NoError(err)

		assert.Equal(disk.PageID(0), tree.GetMetaID())

		// ======================================================================

		// メタチェック
		metaPage, err := poolManager.FetchPage(tree.GetMetaID())
		assert.NoError(err)
		metaNode, err := NewNode(metaPage)
		assert.NoError(err)
		metaData, err := NewMeta(metaNode)
		assert.NoError(err)

		metaPage.Unpin() // メタページをアンピン

		assert.Equal(MetaNodeType, metaNode.getNodeType())
		assert.Equal(disk.PageID(1), metaData.getRootID())

		// ======================================================================

		// ルートチェック
		rootPage, err := poolManager.FetchPage(metaData.getRootID())
		assert.NoError(err)
		rootNode, err := NewNode(rootPage)
		assert.NoError(err)

		assert.Equal(disk.PageID(1), rootPage.GetPageID())
		assert.Equal(LeafNodeType, rootNode.getNodeType())

		// ======================================================================

		// リーフチェック
		leaf, err := NewLeaf(rootNode)
		assert.NoError(err)

		assert.Equal(disk.InvalidPageID, leaf.GetPrevID())
		assert.Equal(disk.InvalidPageID, leaf.GetNextID())
		assert.Equal(uint16(0), leaf.GetNumSlot())
		assert.Equal(uint16(4068), leaf.GetNumFree())

		// ======================================================================

		// テストで作成したページをアンピン
		rootPage.Unpin()

		assert.Equal(pool.Pin(-1), metaPage.GetPinCount())
		assert.Equal(pool.Pin(-1), rootPage.GetPinCount())

		poolManager.Sync()

		// ======================================================================
		// ======================================================================
		// ======================================================================

		// メタチェック
		metaPage, err = poolManager.FetchPage(tree.GetMetaID())
		assert.NoError(err)
		metaNode, err = NewNode(metaPage)
		assert.NoError(err)
		metaData, err = NewMeta(metaNode)
		assert.NoError(err)

		metaPage.Unpin() // メタページをアンピン

		assert.Equal(MetaNodeType, metaNode.getNodeType())
		assert.Equal(disk.PageID(1), metaData.getRootID())

		// ======================================================================

		// ルートチェック
		rootPage, err = poolManager.FetchPage(metaData.getRootID())
		assert.NoError(err)
		rootNode, err = NewNode(rootPage)
		assert.NoError(err)

		assert.Equal(disk.PageID(1), rootPage.GetPageID())
		assert.Equal(LeafNodeType, rootNode.getNodeType())

		// ======================================================================

		// リーフチェック
		leaf, err = NewLeaf(rootNode)
		assert.NoError(err)

		// assert.Equal(uintptr(120), unsafe.Sizeof(*leaf))
		assert.Equal(disk.InvalidPageID, leaf.GetPrevID())
		assert.Equal(disk.InvalidPageID, leaf.GetNextID())
		assert.Equal(uint16(0), leaf.GetNumSlot())
		assert.Equal(uint16(4068), leaf.GetNumFree())

		// ======================================================================

		rootPage.Unpin() // ルートページをアンピン

		assert.Equal(pool.Pin(-1), metaPage.GetPinCount())
		assert.Equal(pool.Pin(-1), rootPage.GetPinCount())

	})

	// t.Run("Insert", func(t *testing.T) {
	// 	// テストデータ準備
	// 	testKey := []byte("key1")
	// 	testValue := []byte("value1")

	// 	// ファイルマネージャ準備
	// 	testFile := "testfile"
	// 	fileManager, err := disk.NewFileManager(testFile)
	// 	assert.NoError(err)
	// 	defer os.Remove(testFile)

	// 	// プール準備
	// 	testPool := pool.NewPool(10)

	// 	// プールマネージャ準備
	// 	poolManager := pool.NewPoolManager(fileManager, testPool)
	// 	defer poolManager.Close()

	// 	// BTree準備
	// 	tree, err := NewBTree(poolManager)
	// 	assert.NoError(err)

	// 	// ======================================================================

	// 	// 挿入
	// 	err = tree.Insert(testKey, testValue)
	// 	assert.NoError(err)

	// 	// 検索（キー）
	// 	itr, err := tree.Search(&btree.Key{key: testKey})
	// 	assert.NoError(err)
	// 	// Get
	// 	resultKey, resultValue, err := itr.Get()
	// 	assert.NoError(err)
	// 	assert.Equal(testKey, resultKey)
	// 	assert.Equal(testValue, resultValue)
	// })

	// t.Run("Insert and Search", func(t *testing.T) {

	// 	// ファイルマネージャ準備
	// 	testFile := "testfile"
	// 	fileManager, err := disk.NewFileManager(testFile)
	// 	assert.NoError(err)
	// 	defer os.Remove(testFile)

	// 	// プール準備
	// 	testPool := pool.NewPool(10)

	// 	// プールマネージャ準備
	// 	poolManager := pool.NewPoolManager(fileManager, testPool)
	// 	defer poolManager.Close()

	// 	// テストデータ準備
	// 	testKey := []byte("key1")
	// 	testValue := []byte("value1")

	// 	// BTree準備
	// 	tree, err := btree.NewBTree(poolManager)
	// 	assert.NoError(err)

	// 	// 挿入
	// 	err = tree.Insert(testKey, testValue)
	// 	assert.NoError(err)

	// 	// 検索（キー）
	// 	itr, err := tree.Search(&btree.Key{key: testKey})
	// 	assert.NoError(err)
	// 	// Get
	// 	resultKey, resultValue, err := itr.Get()
	// 	assert.NoError(err)
	// 	assert.Equal(testKey, resultKey)
	// 	assert.Equal(testValue, resultValue)
	// })
}
