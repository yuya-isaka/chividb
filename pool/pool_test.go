package pool

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/disk"
)

// func TestMain(m *testing.M) {
// 	goleak.VerifyTestMain(m)
// }

// ページ作って、bytesで初期化したデータを用意する。Unpinして返す。
func createPageTest(poolManager *PoolManager, bytes []byte) (disk.PageID, error) {
	// ページ作成
	pageID, err := poolManager.CreatePage()
	if err != nil {
		return disk.InvalidPageID, err
	}

	// ページデータ書き込み
	fetchPage, err := poolManager.FetchPage(pageID)
	if err != nil {
		return disk.InvalidPageID, err
	}

	fetchPage.SetPageData(bytes)
	fetchPage.Unpin()

	return pageID, nil
}

// ======================================================================

func TestPool(t *testing.T) {
	// 準備
	assert := assert.New(t)

	// テストデータ準備
	helloBytes := make([]byte, disk.PageSize)
	copy(helloBytes, "Hello")
	worldBytes := make([]byte, disk.PageSize)
	copy(worldBytes, "World")

	// ======================================================================

	t.Run("Simple Pool 3", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// プール準備
		poolTest := NewPool(3)
		poolManager := NewPoolManager(fileManager, poolTest)
		defer poolManager.Close()

		// ======================================================================

		// create (hello)
		helloID, err := createPageTest(poolManager, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetPageData())
	})

	t.Run("Complex Pool 3", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// プール準備
		poolTest := NewPool(3)
		poolManager := NewPoolManager(fileManager, poolTest)
		defer poolManager.Close()

		// ======================================================================

		// create (hello)
		helloID, err := createPageTest(poolManager, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetPageData())

		// ======================================================================

		// create (world)
		worldID, err := createPageTest(poolManager, worldBytes)
		assert.NoError(err)

		// ======================================================================

		// fetch (hello)
		fetchPage, err = poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetPageData())

		// ======================================================================

		// fetch (world)
		fetchPage, err = poolManager.FetchPage(worldID)
		assert.NoError(err)

		// テスト (world)
		assert.Equal(disk.PageID(1), worldID)
		assert.Equal(worldBytes, fetchPage.GetPageData())
	})

	t.Run("Pool 1", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// プール準備
		poolTest := NewPool(1)
		poolManager := NewPoolManager(fileManager, poolTest)
		defer poolManager.Close()

		// ======================================================================

		// create (hello)
		helloID, err := createPageTest(poolManager, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetPageData())

		// ======================================================================

		// Error test
		// プールのサイズは１で、fetchPageがまだ持っているので、エラーになる
		_, err = poolManager.CreatePage()
		assert.Error(err)
		assert.Equal("all pages are pinned", err.Error())

		// 参照カウンタを減らすことで、新しいページが作れるようになる
		// helloPageとfetchPageは同じページを参照しており、そのページのカウントを２回下げることで-1になる
		fetchPage.Unpin()
		assert.Equal(Pin(-1), fetchPage.GetPinCount())

		// ======================================================================

		// create (world)
		worldID, err := createPageTest(poolManager, worldBytes)
		assert.NoError(err)

		// fetch (world)
		fetchPage, err = poolManager.FetchPage(worldID)
		assert.NoError(err)

		// テスト (world)
		assert.Equal(disk.PageID(1), worldID)
		assert.Equal(worldBytes, fetchPage.GetPageData())

		// ======================================================================

		// Error test
		_, err = poolManager.CreatePage()
		assert.Error(err)
		assert.Equal("all pages are pinned", err.Error())

		fetchPage.Unpin()
		assert.Equal(NoReferencePin, fetchPage.GetPinCount())

		// ======================================================================

		// helloIDはコピーされているので０のままのはず
		assert.Equal(disk.PageID(0), helloID)

		// helloが格納されているpageIDは変わらない
		fetchPage, err = poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(helloBytes, fetchPage.GetPageData())
	})

	t.Run("Pool 2", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// プール準備
		poolTest := NewPool(2)
		poolManager := NewPoolManager(fileManager, poolTest)
		defer poolManager.Close()

		// ======================================================================

		// create (hello)
		helloID, err := createPageTest(poolManager, helloBytes)
		assert.NoError(err)

		// fetch (hello)
		fetchPage, err := poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(disk.PageID(0), helloID)
		assert.Equal(helloBytes, fetchPage.GetPageData())

		// ======================================================================

		// create (world)
		worldID, err := createPageTest(poolManager, worldBytes)
		assert.NoError(err)

		// fetch (world)
		fetchPage, err = poolManager.FetchPage(worldID)
		assert.NoError(err)

		// テスト (world)
		assert.Equal(disk.PageID(1), worldID)
		assert.Equal(worldBytes, fetchPage.GetPageData())

		// ======================================================================

		// fetch (hello)
		fetchPage, err = poolManager.FetchPage(helloID)
		assert.NoError(err)

		// テスト (hello)
		assert.Equal(helloBytes, fetchPage.GetPageData())
	})

	t.Run("Fetch Nonexistent Page", func(t *testing.T) {
		// テストファイル準備
		testFile := "testfile"
		fileManager, err := disk.NewFileManager(testFile)
		assert.NoError(err)
		defer os.Remove(testFile)

		// プール準備
		poolTest := NewPool(2)
		poolManager := NewPoolManager(fileManager, poolTest)
		defer poolManager.Close()

		// ======================================================================

		// 未定義ページIDに対してFetchを行います。
		nonexistentPageID := disk.PageID(999)
		_, err = poolManager.FetchPage(nonexistentPageID)

		assert.Error(err)
		assert.Equal("invalid page id: got 999", err.Error())

		// ======================================================================

		// 未定義ページIDに対してFetchを行います。
		nonexistentPageID = disk.InvalidPageID
		_, err = poolManager.FetchPage(nonexistentPageID)

		assert.Error(err)
		assert.Equal("invalid page id: got -1", err.Error())
	})

}
