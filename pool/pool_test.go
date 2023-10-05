package pool_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/pool"
)

func TestPool(t *testing.T) {
	// 準備
	assert := assert.New(t)

	// テストファイル準備
	testFile := "testfile"
	fileManager, err := disk.NewFileManager(testFile)
	assert.NoError(err)
	defer fileManager.Close()
	defer os.Remove(testFile)

	// テストデータ準備
	helloBytes := make([]byte, disk.PageSize)
	copy(helloBytes, "Hello")
	worldBytes := make([]byte, disk.PageSize)
	copy(worldBytes, "World")

	t.Run("Create and Fetch test", func(t *testing.T) {
		// プール準備
		poolTest := pool.NewPool(3)
		poolManager := pool.NewPoolManager(fileManager, poolTest)

		// create
		helloPage, err := poolManager.CreatePage()
		assert.NoError(err)
		helloPage.SetData(helloBytes)
		helloPage.SetDirty(true)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloPage.GetID())
		assert.NoError(err)
		assert.Equal(disk.PageID(0), helloPage.GetID())
		assert.Equal(helloBytes, fetchPage.GetData())

		// create
		worldPage, err := poolManager.CreatePage()
		assert.NoError(err)
		worldPage.SetData(worldBytes)
		worldPage.SetDirty(true)

		// fetch (hello)
		fetchPage, err = poolManager.FetchPage(helloPage.GetID())
		assert.NoError(err)
		assert.Equal(disk.PageID(0), helloPage.GetID())
		assert.Equal(helloBytes, fetchPage.GetData())

		// fetch (world)
		fetchPage, err = poolManager.FetchPage(worldPage.GetID())
		assert.NoError(err)
		assert.Equal(disk.PageID(1), worldPage.GetID())
		assert.Equal(worldBytes, fetchPage.GetData())
	})
}
