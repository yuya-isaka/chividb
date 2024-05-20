package disk

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWrite(t *testing.T) {
	// 準備
	assert := assert.New(t)

	// テストデータ準備
	helloByte := make([]byte, 4096)
	copy(helloByte, "Hello")
	worldByte := make([]byte, 4096)
	copy(worldByte, "World")

	t.Run("Read and Write Single Page", func(t *testing.T) {

		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Heap.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 書き込み
		testPageID, err := fileManager.AllocPage()
		assert.NoError(err)
		err = fileManager.WriteData(testPageID, helloByte)
		assert.NoError(err)

		// 読み込み
		readBuffer := make([]byte, 4096)
		err = fileManager.ReadData(testPageID, readBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, readBuffer)
	})

	t.Run("Write Write Read Read", func(t *testing.T) {

		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Heap.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 書き込み
		helloPageID, err := fileManager.AllocPage()
		assert.NoError(err)
		err = fileManager.WriteData(helloPageID, helloByte)
		assert.NoError(err)

		// 書き込み
		worldPageID, err := fileManager.AllocPage()
		assert.NoError(err)
		err = fileManager.WriteData(worldPageID, worldByte)
		assert.NoError(err)

		// 読み込み
		helloBuffer := make([]byte, 4096)
		err = fileManager.ReadData(helloPageID, helloBuffer)
		assert.NoError(err)

		// 読み込み
		worldBuffer := make([]byte, 4096)
		err = fileManager.ReadData(worldPageID, worldBuffer)
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
		defer fileManager.Heap.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 書き込み
		helloPageID, err := fileManager.AllocPage()
		assert.NoError(err)
		err = fileManager.WriteData(helloPageID, helloByte)
		assert.NoError(err)

		// 読み込み
		helloBuffer := make([]byte, 4096)
		err = fileManager.ReadData(helloPageID, helloBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(helloByte, helloBuffer)

		// ======================================================================

		// 書き込み
		worldPageID, err := fileManager.AllocPage()
		assert.NoError(err)
		err = fileManager.WriteData(worldPageID, worldByte)
		assert.NoError(err)

		// 読み込み
		worldBuffer := make([]byte, 4096)
		err = fileManager.ReadData(worldPageID, worldBuffer)
		assert.NoError(err)

		// テスト
		assert.Equal(worldByte, worldBuffer)
	})

	t.Run("Error Handling: Read Non-Existent Page", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Heap.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 読み込み: 存在しないページIDを指定
		nonExistentPageID := PageID(-1)
		errBuffer := make([]byte, 4096)
		err = fileManager.ReadData(nonExistentPageID, errBuffer)

		// テスト: エラーが返されるか
		assert.Error(err)
		assert.Equal("ページIDが無効です。指定されたページID: -1", err.Error())

		// ======================================================================

		// 読み込み: 存在しないページIDを指定
		nonExistentPageID = PageID(0)
		errBuffer = make([]byte, 4096)
		err = fileManager.ReadData(nonExistentPageID, errBuffer)

		// テスト: エラーが出て空のページが返されるか
		assert.Error(err)
		assert.Equal("ページIDが無効です。指定されたページID: 0", err.Error())
	})

	t.Run("Error Handling: Write Non-Existent Page", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := NewFileManager(testFile)
		assert.NoError(err)
		defer fileManager.Heap.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 書き込み: 存在しないページIDを指定
		nonExistentPageID := PageID(-1)
		err = fileManager.WriteData(nonExistentPageID, helloByte)

		// テスト: エラーが返されるか
		assert.Error(err)
		assert.Equal("ページIDが無効です。指定されたページID: -1", err.Error())
	})
}

//=================================================================================

func TestFileManager(t *testing.T) {
	// テスト用の一時ファイルを作成
	testPath := "test_heap.file"
	defer os.Remove(testPath) // テスト後にファイルを削除

	// FileManagerのインスタンスを生成
	fm, err := NewFileManager(testPath)
	if err != nil {
		t.Fatalf("Failed to create FileManager: %v", err)
	}

	// 新しいページを割り当てる
	pageID, err := fm.AllocPage()
	if err != nil {
		t.Fatalf("Failed to allocate a new page: %v", err)
	}

	// データの書き込み
	testData := bytes.Repeat([]byte{0xAB}, 4096)
	if err := fm.WriteData(pageID, testData); err != nil {
		t.Fatalf("Failed to write data to page: %v", err)
	}

	// データの読み込み
	readData := make([]byte, 4096)
	if err := fm.ReadData(pageID, readData); err != nil {
		t.Fatalf("Failed to read data from page: %v", err)
	}

	// 書き込んだデータと読み込んだデータが等しいか確認
	if !bytes.Equal(testData, readData) {
		t.Errorf("Mismatch between written and read data.")
	}

	// 不正なページIDでの読み書きのテスト
	invalidPageID := PageID(-1)
	if err := fm.ReadData(invalidPageID, make([]byte, 4096)); err == nil {
		t.Errorf("Expected error for invalid read pageID, got none")
	}
	if err := fm.WriteData(invalidPageID, make([]byte, 4096)); err == nil {
		t.Errorf("Expected error for invalid write pageID, got none")
	}
}
