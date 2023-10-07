package disk

import (
	"fmt"
	"io"
	"os"
)

const (
	PageSize      = 4096       // 1ページサイズ、バイト単位
	InvalidPageID = PageID(-1) // 無効なページID
)

// ページIDを示す型
type PageID int64

// ======================================================================

// ファイルマネージャ構造体
type FileManager struct {
	heap   *os.File // ヒープファイルへのファイルポインタ
	nextID PageID   // 次に割り当てるページID
}

// ファイルマネージャの生成
func NewFileManager(path string) (*FileManager, error) {

	// ファイルオブジェクトの生成
	heap, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0755)
	if err != nil {
		return nil, err
	}

	// ファイルサイズの取得
	info, err := heap.Stat()
	if err != nil {
		return nil, err
	}
	heapSize := info.Size()

	// ファイルサイズのバリデーション
	if heapSize%PageSize != 0 {
		return nil, fmt.Errorf("invalid heap file size: got %d", heapSize)
	}

	// 次に割り当てるページIDの計算とバリデーション
	nextID := PageID(heapSize) / PageSize
	if nextID <= InvalidPageID {
		return nil, fmt.Errorf("invalid page id: got %d", nextID)
	}

	// FileManagerの生成と初期化
	return &FileManager{
		heap:   heap,
		nextID: nextID,
	}, nil
}

// パラメータチェックとファイルポインタの移動を行う補助関数
func (m *FileManager) checkSeek(pageID PageID, pageData []byte) error {

	// ページデータサイズのバリデーション
	if len(pageData) != PageSize {
		return fmt.Errorf("invalid page size: got %d, want %d", len(pageData), PageSize)
	}

	// ページIDのバリデーション
	if pageID <= InvalidPageID {
		return fmt.Errorf("invalid page id: got %d", pageID)
	}

	// ファイルポインタの移動
	if _, err := m.heap.Seek(int64(pageID*PageSize), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek page data: %w", err)
	}

	return nil
}

// 指定ページIDのデータ読み込みを行う関数
func (m *FileManager) ReadPageData(pageID PageID, pageData []byte) error {

	// パラメータチェックとファイルポインタの移動
	if err := m.checkSeek(pageID, pageData); err != nil {
		return err
	}

	// ファイルからデータの読み込み
	if _, err := m.heap.Read(pageData); err != nil {
		return fmt.Errorf("failed to read page data: %w", err)
	}

	return nil
}

// 指定ページIDへデータを書き込む関数
func (m *FileManager) WritePageData(pageID PageID, pageData []byte) error {

	// パラメータチェックとファイルポインタの移動
	if err := m.checkSeek(pageID, pageData); err != nil {
		return err
	}

	// データのファイルへの書き込み
	if _, err := m.heap.Write(pageData); err != nil {
		return fmt.Errorf("failed to write page data: %w", err)
	}

	return nil
}

// 新しいページを割り当てる関数
func (m *FileManager) AllocNewPage() (PageID, error) {
	// 新しいページIDを割り当てて次のIDを更新
	pageID := m.nextID
	m.nextID++
	return pageID, nil
}

// ファイルの変更をディスクに強制的に書き込む関数
func (m *FileManager) Sync() error {
	return m.heap.Sync()
}

// ファイルを閉じる関数
func (m *FileManager) Close() error {
	return m.heap.Close()
}
