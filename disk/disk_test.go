package disk_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/disk"
)

func TestReadWrite(t *testing.T) {
	// 準備
	assert := assert.New(t)

	// テストファイル準備
	testFile := "testfile"
	fileManager, err := disk.NewFileManager(testFile)
	assert.NoError(err)
	defer fileManager.Close()
	defer os.Remove(testFile)

	// テストデータ準備
	helloByte := make([]byte, disk.PageSize)
	copy(helloByte, "Hello")
	worldByte := make([]byte, disk.PageSize)
	copy(worldByte, "World")

	t.Run("Read and Write Single Page", func(t *testing.T) {
		// 書き込み
		testPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(testPageID, helloByte)
		assert.NoError(err)

		// 読み込み
		readBuffer := make([]byte, disk.PageSize)
		err = fileManager.ReadPageData(testPageID, readBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, readBuffer)
	})

	t.Run("Read and Write Multi Page", func(t *testing.T) {
		// 書き込み
		helloPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(helloPageID, helloByte)
		assert.NoError(err)

		// 書き込み
		worldPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(worldPageID, worldByte)
		assert.NoError(err)

		// 読み込み
		helloBuffer := make([]byte, disk.PageSize)
		err = fileManager.ReadPageData(helloPageID, helloBuffer)
		assert.NoError(err)

		// 読み込み
		worldBuffer := make([]byte, disk.PageSize)
		err = fileManager.ReadPageData(worldPageID, worldBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, helloBuffer)
		assert.Equal(worldByte, worldBuffer)
	})

	t.Run("Read and Write Multi Page", func(t *testing.T) {
		// 書き込み
		helloPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(helloPageID, helloByte)
		assert.NoError(err)

		// 読み込み
		helloBuffer := make([]byte, disk.PageSize)
		err = fileManager.ReadPageData(helloPageID, helloBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, helloBuffer)

		// 書き込み
		worldPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(worldPageID, worldByte)
		assert.NoError(err)

		// 読み込み
		worldBuffer := make([]byte, disk.PageSize)
		err = fileManager.ReadPageData(worldPageID, worldBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(worldByte, worldBuffer)
	})
}
