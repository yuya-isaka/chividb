package disk

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestMain(m *testing.M) {
// 	goleak.VerifyTestMain(m)
// }

func TestReadWrite(t *testing.T) {
	// 準備
	assert := assert.New(t)

	// テストデータ準備
	helloByte := make([]byte, PageSize)
	copy(helloByte, "Hello")
	worldByte := make([]byte, PageSize)
	copy(worldByte, "World")

	t.Run("Read and Write Single Page", func(t *testing.T) {

		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 書き込み
		testPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(testPageID, helloByte)
		assert.NoError(err)

		// 読み込み
		readBuffer := make([]byte, PageSize)
		err = fileManager.ReadPageData(testPageID, readBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, readBuffer)
	})

	t.Run("Write Write Read Read", func(t *testing.T) {

		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

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
		helloBuffer := make([]byte, PageSize)
		err = fileManager.ReadPageData(helloPageID, helloBuffer)
		assert.NoError(err)

		// 読み込み
		worldBuffer := make([]byte, PageSize)
		err = fileManager.ReadPageData(worldPageID, worldBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, helloBuffer)
		assert.Equal(worldByte, worldBuffer)
	})

	t.Run("Write Read Write Read", func(t *testing.T) {

		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 書き込み
		helloPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(helloPageID, helloByte)
		assert.NoError(err)

		// 読み込み
		helloBuffer := make([]byte, PageSize)
		err = fileManager.ReadPageData(helloPageID, helloBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, helloBuffer)

		// ======================================================================

		// 書き込み
		worldPageID, err := fileManager.AllocNewPage()
		assert.NoError(err)
		err = fileManager.WritePageData(worldPageID, worldByte)
		assert.NoError(err)

		// 読み込み
		worldBuffer := make([]byte, PageSize)
		err = fileManager.ReadPageData(worldPageID, worldBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(worldByte, worldBuffer)
	})

	t.Run("Error Handling: Read Non-Existent Page", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 読み込み: 存在しないページIDを指定
		nonExistentPageID := InvalidPageID
		errBuffer := make([]byte, PageSize)
		err = fileManager.ReadPageData(nonExistentPageID, errBuffer)

		// テスト: エラーが返されるか
		assert.Error(err)
		assert.Equal("invalid page id: got -1", err.Error())

		// ======================================================================

		// 読み込み: 存在しないページIDを指定
		nonExistentPageID = PageID(0)
		errBuffer = make([]byte, PageSize)
		err = fileManager.ReadPageData(nonExistentPageID, errBuffer)

		// テスト: エラーが出て空のページが返されるか
		assert.Error(err)
		assert.Equal("invalid page id: got 0", err.Error())
	})

	t.Run("Error Handling: Write Non-Existent Page", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 書き込み: 存在しないページIDを指定
		nonExistentPageID := InvalidPageID
		err = fileManager.WritePageData(nonExistentPageID, helloByte)

		// テスト: エラーが返されるか
		assert.Error(err)
		assert.Equal("invalid page id: got -1", err.Error())
	})
}
