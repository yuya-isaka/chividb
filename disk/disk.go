package disk

import (
	"fmt"
	"io"
	"os"
)

const (
	PageSize  = 4096       // 1ページのサイズをバイト単位で定義
	InvalidID = PageID(-1) // 無効なページIDを定義
)

// ページIDとして64ビット整数型を定義
type PageID int64

// ======================================================================

// ファイルの管理を抽象化した構造体
type FileManager struct {
	heap   *os.File // ファイル操作用のオブジェクト
	nextID PageID   // 次に割り当てるページID
}

// 新しいFileManagerの生成と初期化を行う関数
//
//	path: 使用するファイルのパス
//	返り値1: 初期化されたFileManagerオブジェクト
//	返り値2: 初期化時に発生した可能性のあるエラー
func NewFileManager(path string) (*FileManager, error) {

	// ファイルオブジェクトの生成とエラーチェック
	heap, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0755)
	if err != nil {
		return nil, err
	}

	// ファイルサイズの取得とエラーチェック
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
	if nextID <= InvalidID {
		return nil, fmt.Errorf("invalid page id: got %d", nextID)
	}

	// FileManagerの生成と初期化
	return &FileManager{
		heap:   heap,
		nextID: nextID,
	}, nil
}

// ファイルポインタの移動とパラメータチェックを行う補助関数
//
//	pageID: チェックまたは移動したいページのID
//	pageData: 読み書きするデータのバイトスライス
//	返り値1: ファイルポインタの移動やパラメータチェック中に発生した可能性のあるエラー
func (m *FileManager) checkSeek(pageID PageID, pageData []byte) error {

	// ページデータサイズのバリデーション
	if len(pageData) != PageSize {
		return fmt.Errorf("invalid page size: got %d, want %d", len(pageData), PageSize)
	}

	// ページIDのバリデーション
	if pageID <= InvalidID {
		return fmt.Errorf("invalid page id: got %d", pageID)
	}

	// ファイルポインタの移動
	if _, err := m.heap.Seek(int64(pageID*PageSize), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek page data: %w", err)
	}

	return nil
}

// 指定ページIDのデータ読み込みを行う関数
//
//	pageID: データを読み込むページのID
//	pageData: 読み込んだデータを格納するバイトスライス
//	返り値1: データ読み込み中に発生した可能性のあるエラー
func (m *FileManager) ReadPageData(pageID PageID, pageData []byte) error {

	// ファイルポインタの移動とパラメータチェック
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
//
//	pageID: データを書き込むページのID
//	pageData: 書き込むデータを含むバイトスライス
//	返り値1: データ書き込み中に発生した可能性のあるエラー
func (m *FileManager) WritePageData(pageID PageID, pageData []byte) error {

	// ファイルポインタの移動とパラメータチェック
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
//
//	返り値1: 割り当てられた新しいページID
//	返り値2: ページ割り当て中に発生した可能性のあるエラー
func (m *FileManager) AllocateNewPage() (PageID, error) {
	// 新しいページIDを割り当てて次のIDを更新
	pageID := m.nextID
	m.nextID++
	return pageID, nil
}

// ファイルの変更をディスクに強制的に書き込む関数
//
//	返り値1: ファイル同期中に発生した可能性のあるエラー
func (m *FileManager) Sync() error {
	return m.heap.Sync()
}

// ファイルを閉じる関数
//
//	返り値1: ファイルクローズ中に発生した可能性のあるエラー
func (m *FileManager) Close() error {
	return m.heap.Close()
}
