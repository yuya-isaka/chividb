package btree_test

import (
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/btree"
	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/pool"
)

func TestBTree_InsertAndSearch(t *testing.T) {

	// 準備
	assert := assert.New(t)

	t.Run("Create BTree", func(t *testing.T) {

		// ファイルマネージャ準備
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// プール準備
		testPool := pool.NewPool(10)

		// プールマネージャ準備
		poolManager := pool.NewPoolManager(fileManager, testPool)
		defer poolManager.Close()

		// ======================================================================

		// BTree準備
		// TODO ここでtreeとルートIDを渡せばいい？ (メタデータはユーザは意識しなくていい？)
		tree, err := btree.NewBTree(poolManager)
		assert.NoError(err)

		assert.Equal(disk.PageID(0), tree.GetMetaID())

		// ======================================================================

		// メタページ取得
		metaPage, err := poolManager.FetchPage(tree.GetMetaID())
		assert.NoError(err)

		// メタデータ生成
		metaData, err := btree.NewMeta(metaPage)
		assert.NoError(err)
		assert.Equal(uintptr(24), unsafe.Sizeof(*metaData))

		// メタデータからルートID取得
		// ルートページ取得
		rootPage, err := poolManager.FetchPage(metaData.GetRootID())
		assert.NoError(err)

		assert.Equal(disk.PageID(1), rootPage.GetPageID())

		// ======================================================================

		// ルートノード生成
		rootNode, err := btree.NewNode(rootPage)
		assert.NoError(err)
		assert.Equal(uintptr(48), unsafe.Sizeof(*rootNode))

		// ルートノードも最初はリーフタイプ
		assert.Equal(btree.LeafNodeType, rootNode.GetNodeType())

		// ======================================================================

		// リーフノード生成
		leaf, err := btree.NewLeaf(rootNode)
		assert.NoError(err)
		assert.Equal(uintptr(120), unsafe.Sizeof(*leaf))

		// テスト
		assert.Equal(disk.InvalidPageID, leaf.GetPrevID())
		assert.Equal(disk.InvalidPageID, leaf.GetNextID())
		assert.Equal(uint16(0), leaf.GetNumSlots())
		assert.Equal(uint16(4068), leaf.GetFreeSpace())

		// ======================================================================

		// Tree削除
		err = tree.Clear(poolManager)
		assert.NoError(err)

		// テストで作成したページをアンピン
		metaPage.Unpin()
		rootPage.Unpin()

		assert.Equal(pool.Pin(-1), metaPage.GetPinCount())
		assert.Equal(pool.Pin(-1), rootPage.GetPinCount())

	})

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
