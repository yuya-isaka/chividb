package pool

import (
	"errors"
	"fmt"

	"github.com/yuya-isaka/chibidb/disk"
)

const (
	InvalidPoolIndex = PoolIndex(^uint64(0)) // 無効なプールインデックス
	NoReferencePin   = Pin(-1)               // ピンがない状態
)

// プール内のページ位置を示す型
type PoolIndex uint64

// ページの参照カウントを示す型
type Pin int64

// ======================================================================

// ストレージの1ページを表す構造体
type Page struct {
	id     disk.PageID // ページの一意なID
	data   []byte      // ページのデータ内容
	pin    Pin         // ページの参照数
	update bool        // ページの更新フラグ
}

// ページ情報のリセット
func (p *Page) reset() {
	p.id = disk.InvalidPageID            // ページIDを無効化
	p.data = make([]byte, disk.PageSize) // ページデータをデフォルトサイズに初期化
	p.pin = NoReferencePin               // 参照カウントを無効化
	p.update = false                     // 更新フラグをリセット
}

// ページIDの取得
func (p *Page) GetID() disk.PageID {
	return p.id
}

// ページIDの設定
func (p *Page) SetID(newID disk.PageID) {
	p.id = newID
}

// ページデータの取得
func (p *Page) GetData() []byte {
	return p.data
}

// ページデータの設定
func (p *Page) SetData(data []byte) {
	p.data = data
}

// ピンカウントの取得
func (p *Page) GetPinCount() Pin {
	return p.pin
}

// ピンカウントのインクリメント
func (p *Page) addPin() {
	p.pin++
}

// ピンカウントのデクリメント
func (p *Page) Unpin() {
	p.pin--
}

// 更新フラグの取得
func (p *Page) GetUpdate() bool {
	return p.update
}

// 更新フラグの設定
func (p *Page) SetUpdate(update bool) {
	p.update = update
}

// ======================================================================

// 複数のPageをバッファするメモリプール
type Pool struct {
	pages         []Page    // プール内の全ページ
	nextKickIndex PoolIndex // 次にプールから削除するページのインデックス
}

// 指定されたサイズの新しいページプールの作成
func NewPool(size int) *Pool {

	// 一定数のページを持つプールを作成し、各ページを初期化
	// （辞書アクセスでバグらせないように）
	pages := make([]Page, size)
	for i := 0; i < size; i++ {
		pages[i].reset()
	}

	return &Pool{
		pages:         pages,
		nextKickIndex: PoolIndex(0),
	}
}

// 指定インデックスのページの取得
func (p *Pool) getPage(index PoolIndex) *Page {
	return &p.pages[index]
}

// クロックスイープアルゴリズム: プールからページを削除するインデックスの探索
func (p *Pool) clockSweep() (PoolIndex, error) {
	pageNum := len(p.pages) // プール内のページ数
	checkedPageNum := 0     // チェックしたページ数

	// プールのページを探し、ピンされていないページを見つけるか、全ページをチェックするまでループ
	for {
		nextKickIndex := p.nextKickIndex
		page := p.getPage(nextKickIndex)

		if page.GetPinCount() == NoReferencePin {
			// ピンされていないページを見つけたら、そのインデックスを返す
			return nextKickIndex, nil
		} else {
			checkedPageNum++
			if checkedPageNum >= pageNum {
				// 全てのページがピンされている場合はエラーを返す
				return 0, errors.New("all pages are pinned")
			}
		}

		// 次のチェックするインデックスの準備
		p.nextKickIndex = (p.nextKickIndex + 1) % PoolIndex(pageNum)
	}
}

// ======================================================================

// ページプールとページテーブルを管理
type PoolManager struct {
	fileManager *disk.FileManager         // データのファイルへの保存・読み込みを行うマネージャ
	pool        *Pool                     // ページプール
	pageTable   map[disk.PageID]PoolIndex // ページIDとプール内のインデックスをマッピングするテーブル
}

// 新しいPoolManagerを作成
func NewPoolManager(fileManager *disk.FileManager, pool *Pool) *PoolManager {
	return &PoolManager{
		fileManager: fileManager,
		pool:        pool,
		pageTable:   make(map[disk.PageID]PoolIndex),
	}
}

// プールからページを削除し、その場所を新しいページで利用可能にする
func (p *PoolManager) kickPage() (*Page, PoolIndex, error) {

	// プールから使用可能なページインデックスを探索
	poolIndex, err := p.pool.clockSweep()
	if err != nil {
		return nil, InvalidPoolIndex, err
	}

	// ページがページテーブルに登録されていれば、登録を削除
	page := p.pool.getPage(poolIndex)
	delete(p.pageTable, page.GetID())

	// ページが更新されていれば、その内容をファイルに書き込み
	if page.GetUpdate() {
		if err := p.fileManager.WritePageData(page.GetID(), page.GetData()); err != nil {
			return nil, InvalidPoolIndex, err
		}
	}

	// 使用可能なページとそのインデックスを返却
	return page, poolIndex, nil
}

// 指定したページIDのページを取得
func (p *PoolManager) FetchPage(pageID disk.PageID) (*Page, error) {

	// 無効なページIDはエラー
	if pageID <= disk.InvalidPageID {
		return nil, fmt.Errorf("invalid page id: got %d", pageID)
	}

	// ページテーブルにページIDのページが存在するか確認
	if poolIndex, ok := p.pageTable[pageID]; ok {
		page := p.pool.getPage(poolIndex)
		page.addPin()    // ページ利用のためピンカウントを増加
		return page, nil // 存在すれば、そのページを返却
	}

	// ページテーブルに存在しなければ、プールからページを取得しファイルから内容を読み込み
	page, poolIndex, err := p.kickPage()
	if err != nil {
		return nil, err
	}
	page.SetID(pageID)
	page.SetUpdate(false)
	p.fileManager.ReadPageData(pageID, page.GetData()) // ファイルからページデータを読み込み
	page.addPin()                                      // ページ利用のためピンカウントを増加
	p.pageTable[pageID] = poolIndex

	return page, nil
}

// 新しいページを作成
func (p *PoolManager) CreatePage() (disk.PageID, error) {

	// プールから使用可能なページを取得
	page, poolIndex, err := p.kickPage()
	if err != nil {
		return disk.InvalidPageID, err
	}

	// ファイルから新しいページIDを取得
	newPageID, err := p.fileManager.AllocNewPage()
	if err != nil {
		return disk.InvalidPageID, err
	}

	// 新しいページの設定を行い、ページテーブルに登録
	page.reset()
	page.SetID(newPageID)
	page.SetUpdate(true)
	p.pageTable[newPageID] = poolIndex

	return newPageID, nil
}

// ページテーブル内の変更されたすべてのページをファイルに書き込み
func (p *PoolManager) Sync() error {
	for pageId, poolIndex := range p.pageTable {
		page := p.pool.getPage(poolIndex)
		if err := p.fileManager.WritePageData(pageId, page.GetData()); err != nil {
			return err
		}
		page.SetUpdate(false)
	}

	// ファイル内容をディスクと同期
	return p.fileManager.Sync()
}

// プールマネージャを閉じ、関連リソースを解放
func (p *PoolManager) Close() error {
	// 変更されたページをファイルと同期
	if err := p.Sync(); err != nil {
		return err
	}

	// ファイルマネージャを閉じる
	return p.fileManager.Close()
}
