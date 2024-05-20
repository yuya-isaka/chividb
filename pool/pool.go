package pool

import (
	"fmt"

	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/page"
)

// ページプールとページテーブルを管理
type PoolManager struct {
	fileManager *disk.FileManager    // データのファイルへの保存・読み込みを行うマネージャ
	pool        []*page.Page         // プール内の全ページ
	sweepIndex  uint                 // 次にプールから削除するページのインデックス
	pageTable   map[disk.PageID]uint // ページIDとプール内のインデックスをマッピングするテーブル
}

// 新しいPoolManagerを作成
// エラーは起きないはず
func NewPoolManager(path string, poolNum uint) (*PoolManager, error) {

	fm, err := disk.NewFileManager(path)
	if err != nil {
		return nil, err
	}

	// 一定数のページを持つプールを作成し、各ページを初期化
	// （辞書アクセスでバグらせないように）
	pool := make([]*page.Page, 0, poolNum)
	for range poolNum {
		pool = append(pool, page.NewPage())
	}

	return &PoolManager{
		fileManager: fm,
		pool:        pool,
		sweepIndex:  0,
		pageTable:   make(map[disk.PageID]uint),
	}, nil
}

// プールで使用可能なページとそのインデクスを返却
// クロックスイープアルゴリズム: プールからページを削除するインデックスの探索
// TODO:改良の余地あり
func (pm *PoolManager) sweepPage() (*page.Page, uint, error) {

	// ------------------------------------------------------------------
	for {
		sweepi := pm.sweepIndex
		page := pm.pool[sweepi]

		if page.Counter == 0 {
			// ページがページテーブルに登録されていれば、登録を削除
			delete(pm.pageTable, page.PageID)

			// ページが更新されていれば、その内容をファイルに書き込み
			if page.Flag {
				if err := pm.fileManager.WriteData(page.PageID, page.GetAllData()); err != nil {
					page.Flag = false
					return nil, 0, err
				}
			}

			pm.sweepIndex = (sweepi + 1) % uint(len(pm.pool))

			// このページとインデックス使っていいよー
			return page, sweepi, nil
		}

		page.Counter--
		pm.sweepIndex = (sweepi + 1) % uint(len(pm.pool))
	}
}

// 新しいページを作成し、そのページIDを返却
func (pm *PoolManager) CreatePage() (disk.PageID, error) {

	// プールから使用可能なページを取得
	page, poolIndex, err := pm.sweepPage()
	if err != nil {
		return disk.PageID(-1), err
	}

	page.ResetPage()
	page.ResetPageData() // Flagをtrueにする

	// 新しいページの設定
	newPageID, err := pm.fileManager.AllocPage()
	if err != nil {
		return disk.PageID(-1), err
	}
	page.PageID = newPageID

	// ページテーブルに登録
	pm.pageTable[newPageID] = poolIndex

	return newPageID, nil
}

// 指定したページIDのページを取得し返却
func (pm *PoolManager) FetchPage(pageID disk.PageID) (*page.Page, error) {

	// 無効なページIDはエラー
	if pageID <= disk.PageID(-1) || pageID >= pm.fileManager.NextID {
		return nil, fmt.Errorf("指定されたページIDが無効です。ページID: %d", pageID)
	}

	// ページテーブルにページIDのページが存在するか確認
	if poolIndex, ok := pm.pageTable[pageID]; ok {
		page := pm.pool[poolIndex]
		page.Counter++   // ページ利用のためカウントを増加
		return page, nil // 存在すれば、そのページを返却
	}

	// ページテーブルに存在しなければ、プールからページを取得しファイルから内容を読み込み
	newPage, poolIndex, err := pm.sweepPage()
	if err != nil {
		return nil, err
	}

	// データ初期化------------------------------------------------------
	// ファイルからページデータを読み込み
	if err = pm.fileManager.ReadData(pageID, newPage.GetAllData()); err != nil {
		return nil, err
	}
	newPage.PageID = pageID
	newPage.Flag = false // データは更新されていないのでfalse
	newPage.Counter++    // ページ利用のためカウントを増加
	//-----------------------------------------------------------------

	// ページテーブルに登録
	pm.pageTable[pageID] = poolIndex

	return newPage, nil
}

// ページテーブル内の変更されたすべてのページをファイルに書き込み
func (pm *PoolManager) Sync() error {
	for pageId, poolIndex := range pm.pageTable {
		page := pm.pool[poolIndex]
		if !page.Flag {
			continue
		}
		if err := pm.fileManager.WriteData(pageId, page.GetAllData()); err != nil {
			return err
		}
		page.Flag = false
	}

	// ファイル内容をディスクと同期
	return pm.fileManager.Heap.Sync()
}

// プールマネージャを閉じ、関連リソースを解放
func (pm *PoolManager) Close() error {
	// 変更されたページをファイルと同期
	if err := pm.Sync(); err != nil {
		return err
	}

	// ファイルマネージャを閉じる
	return pm.fileManager.Heap.Close()
}
