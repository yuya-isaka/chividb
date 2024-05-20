package disk

import (
	"fmt"
	"io"
	"os"
)

// ページIDを示す型
type PageID int64

// ======================================================================

// ファイルマネージャ構造体
type FileManager struct {
	Heap   *os.File // ヒープファイルへのファイルポインタ
	NextID PageID   // 次に割り当てるページID
}

// ファイルマネージャの生成
func NewFileManager(path string) (*FileManager, error) {

	// ファイルオブジェクトの生成
	// os.O_SYNCなくてもいいかも
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
	if heapSize%4096 != 0 {
		return nil, fmt.Errorf("ヒープファイルのサイズが無効です。期待されるサイズは4096の倍数ですが、現在のサイズは %d バイトです。", heapSize)
	}

	// 次に割り当てるページIDの計算とバリデーション
	// heapSize==0の場合、nextID==0となる
	// heapSize==4096の場合、nextID==1となる
	// heapSize==8192の場合、nextID==2となる
	nextID := PageID(heapSize) / 4096
	if nextID < 0 {
		return nil, fmt.Errorf("ページIDが無効です。指定されたページID: %d", nextID)
	}

	// FileManagerの生成と初期化
	return &FileManager{
		Heap:   heap,
		NextID: nextID,
	}, nil
}

func (f *FileManager) seekData(pageID PageID) error {
	// ページIDのバリデーション
	if pageID < 0 || pageID >= f.NextID {
		return fmt.Errorf("ページIDが無効です。指定されたページID: %d", pageID)
	}
	// ファイルポインタの移動
	if _, err := f.Heap.Seek(int64(pageID*4096), io.SeekStart); err != nil {
		return fmt.Errorf("ページデータのシークに失敗しました。ページID: %d, エラー詳細: %w", pageID, err)
	}
	return nil
}

// 指定ページIDのデータ読み込みを行う関数
func (f *FileManager) ReadData(pageID PageID, pageData []byte) error {
	// ページデータサイズのバリデーション
	if len(pageData) != 4096 {
		return fmt.Errorf("不正なページサイズが指定されました。現在のサイズ: %d バイト, 要求される正確なサイズ: 4096 バイト", len(pageData))
	}

	// ページIDからデータの位置を特定
	if err := f.seekData(pageID); err != nil {
		return err
	}

	// ファイルからデータの読み込み
	n, err := f.Heap.Read(pageData)
	if err != nil {
		return fmt.Errorf("ページデータの読み込みに失敗しました。ページID: %d, エラー詳細: %w", pageID, err)
	}
	if n != 4096 {
		return fmt.Errorf("ページデータの読み込みに失敗しました。ページID: %d, 読み込まれたバイト数: %d", pageID, n)
	}

	return nil
}

// 指定ページIDへデータを書き込む関数
func (f *FileManager) WriteData(pageID PageID, pageData []byte) error {
	// ページデータサイズのバリデーション
	if len(pageData) != 4096 {
		return fmt.Errorf("不正なページサイズが指定されました。現在のサイズ: %d バイト, 要求される正確なサイズ: 4096 バイト", len(pageData))
	}

	// ページIDからデータの位置を特定
	if err := f.seekData(pageID); err != nil {
		return err
	}

	// データのファイルへの書き込み
	n, err := f.Heap.Write(pageData)
	if err != nil {
		return fmt.Errorf("ページデータの書き込みに失敗しました。ページID: %d, エラー詳細: %w", pageID, err)
	}
	if n != 4096 {
		return fmt.Errorf("ページデータの書き込みに失敗しました。ページID: %d, 書き込まれたバイト数: %d", pageID, n)
	}

	return nil
}

// 新しいページを割り当てる関数
func (f *FileManager) AllocPage() (PageID, error) {
	// 新しいページIDを割り当てて次のIDを更新
	pageID := f.NextID
	f.NextID++
	return pageID, nil
}
