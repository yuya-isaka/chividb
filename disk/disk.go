package disk

import (
	"errors"
	"io"
	"os"
)

const (
	PageSize  = 4096
	InvalidID = PageID(-1)
)

type PageID int64

// ======================================================================

type FileManager struct {
	heap   *os.File
	nextID PageID
}

func NewFileManager(path string) (*FileManager, error) {
	// ファイル準備
	heap, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0755)
	if err != nil {
		return nil, err
	}

	// サイズ確保＆サイズチェック
	info, err := heap.Stat()
	if err != nil {
		return nil, err
	}
	heapSize := info.Size()
	if heapSize%PageSize != 0 {
		return nil, errors.New("invalid heap file size")
	}

	nextID := PageID(heapSize) / PageSize
	if nextID <= InvalidID {
		return nil, errors.New("invalid page id")
	}

	return &FileManager{
		heap:   heap,
		nextID: nextID,
	}, nil
}

func checkPage(pageID PageID, pageData []byte) error {
	// ページサイズチェック
	if len(pageData) != PageSize {
		return errors.New("invalid page size")
	}
	// ページIDチェック
	if pageID <= InvalidID {
		return errors.New("invalid page id")
	}
	return nil
}

// ファイルシーク
func (m *FileManager) seek(pageID PageID) error {
	_, err := m.heap.Seek(int64(pageID*PageSize), io.SeekStart)
	return err
}

// ページデータ読み込み
func (m *FileManager) ReadPageData(pageID PageID, pageData []byte) error {
	// チェック
	err := checkPage(pageID, pageData)
	if err != nil {
		return err
	}

	// ファイルシーク
	err = m.seek(pageID)
	if err != nil {
		return err
	}

	// ファイル読み込み
	_, err = m.heap.Read(pageData)
	if err != nil {
		return err
	}

	return nil
}

// ページデータ書き込み
func (m *FileManager) WritePageData(pageID PageID, pageData []byte) error {
	// チェック
	err := checkPage(pageID, pageData)
	if err != nil {
		return err
	}

	// ファイルシーク
	err = m.seek(pageID)
	if err != nil {
		return err
	}

	// ファイル読み込み
	_, err = m.heap.Write(pageData)
	if err != nil {
		return err
	}

	return nil
}

// ページ割り当て
func (m *FileManager) AllocateNewPage() (PageID, error) {
	pageID := m.nextID
	m.nextID++
	return pageID, nil
}

// ファイル同期
func (m *FileManager) Sync() error {
	return m.heap.Sync()
}

// ファイルクローズ
func (m *FileManager) Close() error {
	return m.heap.Close()
}
