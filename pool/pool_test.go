package pool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/disk"
)

func createSetPage(pm *PoolManager, start uint, data []byte) (disk.PageID, error) {
	pageID, err := pm.CreatePage()
	if err != nil {
		return disk.PageID(-1), err
	}

	page, err := pm.FetchPage(pageID)
	if err != nil {
		return disk.PageID(-1), err
	}

	page.SetData(uint16(start), uint16(len(data)), data)

	return pageID, nil
}

func TestPool(t *testing.T) {
	// 準備
	assert := assert.New(t)

	// テストデータ準備
	helloBytes := make([]byte, 4096)
	copy(helloBytes, "Hello")
	worldBytes := make([]byte, 4096)
	copy(worldBytes, "World")

	// ======================================================================

	t.Run("Simple Pool 3", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		poolManager, err := NewPoolManager(testFile, 3)
		assert.NoError(err)
		defer poolManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// create (hello)
		helloID, err := createSetPage(poolManager, 0, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetAllData())
	})

	t.Run("Complex Pool 3", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		poolManager, err := NewPoolManager(testFile, 3)
		assert.NoError(err)
		defer poolManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// create (hello)
		helloID, err := createSetPage(poolManager, 0, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetAllData())

		// ======================================================================

		// create (world)
		worldID, err := createSetPage(poolManager, 0, worldBytes)
		assert.NoError(err)

		// ======================================================================

		// fetch (hello)
		fetchPage, err = poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetAllData())

		// ======================================================================

		// fetch (world)
		fetchPage, err = poolManager.FetchPage(worldID)
		assert.NoError(err)

		// テスト (world)
		assert.Equal(disk.PageID(1), worldID)
		assert.Equal(worldBytes, fetchPage.GetAllData())
	})

	t.Run("Pool 1", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		poolManager, err := NewPoolManager(testFile, 1)
		assert.NoError(err)
		defer poolManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// create (hello)
		helloID, err := createSetPage(poolManager, 0, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetAllData())

		// ======================================================================

		// create (world)
		worldID, err := createSetPage(poolManager, 0, worldBytes)
		assert.NoError(err)

		// fetch (world)
		fetchPage, err = poolManager.FetchPage(worldID)
		assert.NoError(err)

		// テスト (world)
		assert.Equal(disk.PageID(1), worldID)
		assert.Equal(worldBytes, fetchPage.GetAllData())

		// ======================================================================

		// helloIDはコピーされているので０のままのはず
		assert.Equal(disk.PageID(0), helloID)

		// helloが格納されているpageIDは変わらない
		fetchPage, err = poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(helloBytes, fetchPage.GetAllData())
	})

	t.Run("Pool 2", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		poolManager, err := NewPoolManager(testFile, 2)
		assert.NoError(err)
		defer poolManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// create (hello)
		helloID, err := createSetPage(poolManager, 0, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetAllData())

		// ======================================================================

		// create (world)
		worldID, err := createSetPage(poolManager, 0, worldBytes)
		assert.NoError(err)

		// fetch (world)
		fetchPage, err = poolManager.FetchPage(worldID)
		assert.NoError(err)

		// テスト (world)
		assert.Equal(disk.PageID(1), worldID)
		assert.Equal(worldBytes, fetchPage.GetAllData())

		// ======================================================================

		// fetch (hello)
		fetchPage, err = poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(helloBytes, fetchPage.GetAllData())
	})

	t.Run("Fetch Nonexistent Page", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		poolManager, err := NewPoolManager(testFile, 2)
		assert.NoError(err)
		defer poolManager.Close()
		defer os.Remove(testFile)

		// ======================================================================

		// 未定義ページIDに対してFetchを行います。
		nonexistentPageID := disk.PageID(999)
		_, err = poolManager.FetchPage(nonexistentPageID)

		assert.Error(err)
		assert.Equal("指定されたページIDが無効です。ページID: 999", err.Error())

		// ======================================================================

		// 未定義ページIDに対してFetchを行います。
		nonexistentPageID = disk.PageID(-1)
		_, err = poolManager.FetchPage(nonexistentPageID)

		assert.Error(err)
		assert.Equal("指定されたページIDが無効です。ページID: -1", err.Error())
	})

}

// ==========================================================================

func TestNewPoolManager(t *testing.T) {
	dir := t.TempDir()

	_, err := NewPoolManager(dir+"/dbfile", 10)
	if err != nil {
		t.Errorf("Failed to create PoolManager: %v", err)
	}
}

func TestCreateAndFetchPage(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPoolManager(dir+"/dbfile", 10)
	if err != nil {
		t.Fatalf("Failed to create PoolManager: %v", err)
	}
	defer pm.Close()

	pageID, err := pm.CreatePage()
	if err != nil {
		t.Errorf("Failed to create page: %v", err)
	}

	page, err := pm.FetchPage(pageID)
	if err != nil {
		t.Errorf("Failed to fetch page: %v", err)
	}

	if page.PageID != pageID {
		t.Errorf("Failed pageID: %v", err)
	}
}

func TestSyncAndClose(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPoolManager(dir+"/dbfile", 10)
	if err != nil {
		t.Fatalf("Failed to create PoolManager: %v", err)
	}

	// Perform some operations
	pageID, _ := pm.CreatePage()
	page, _ := pm.FetchPage(pageID)
	page.SetData(0, uint16(0+len("some data")), []byte("some data"))

	// Sync and close
	if err := pm.Sync(); err != nil {
		t.Errorf("Failed to sync: %v", err)
	}
	if err := pm.Close(); err != nil {
		t.Errorf("Failed to close PoolManager: %v", err)
	}
}
