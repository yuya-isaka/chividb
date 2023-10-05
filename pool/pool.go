package pool

import (
	"errors"

	"github.com/yuya-isaka/chibidb/disk"
)

const (
	InvalidIndex   = PoolIndex(^uint64(0))
	NoReferencePin = Pin(-1)
)

type PoolIndex uint64
type Pin int64

type Page struct {
	id     disk.PageID
	data   []byte
	pin    Pin
	update bool
}

func (p *Page) reset() {
	p.id = disk.InvalidID
	p.data = make([]byte, disk.PageSize)
	p.pin = NoReferencePin
	p.update = false
}

func (p *Page) GetID() disk.PageID {
	return p.id
}

func (p *Page) SetID(id disk.PageID) {
	p.id = id
}

func (p *Page) GetData() []byte {
	return p.data
}

func (p *Page) SetData(data []byte) {
	p.data = data
}

func (p *Page) GetPinCount() Pin {
	return p.pin
}

// これだけ非公開で一旦運用してみる
func (p *Page) addPin() {
	p.pin++
}

func (p *Page) SubPin() {
	p.pin--
}

func (p *Page) GetUpdate() bool {
	return p.update
}

func (p *Page) SetUpdate(update bool) {
	p.update = update
}

// ======================================================================

type Pool struct {
	pages         []Page
	nextKickIndex PoolIndex
}

func NewPool(size int) *Pool {
	// Pageの初期化 (辞書アクセスでバグらんように)
	pages := make([]Page, size)
	for i := 0; i < size; i++ {
		pages[i].reset()
	}

	return &Pool{
		pages:         pages,
		nextKickIndex: PoolIndex(0),
	}
}

func (p *Pool) getPage(index PoolIndex) *Page {
	return &p.pages[index]
}

func (p *Pool) clockSweep() (PoolIndex, error) {
	pageNum := len(p.pages)

	checkedPageNum := 0

	for {
		nextKickIndex := p.nextKickIndex
		page := p.getPage(nextKickIndex)

		if page.GetPinCount() == NoReferencePin {
			return nextKickIndex, nil
		} else {
			checkedPageNum++
			if checkedPageNum >= pageNum {
				// ここでエラーを返さずに何かを通知する処理を追加したら、良いのかな？ (プールのサイズとスレッドの数を一致させたら色々と都合が良い？)
				return 0, errors.New("all pages are pinned")
			}
		}

		p.nextKickIndex = (p.nextKickIndex + 1) % PoolIndex(pageNum)
	}
}

// ======================================================================

type PoolManager struct {
	fileManager *disk.FileManager
	pool        *Pool
	pageTable   map[disk.PageID]PoolIndex
}

func NewPoolManager(fileManager *disk.FileManager, pool *Pool) *PoolManager {
	return &PoolManager{
		fileManager: fileManager,
		pool:        pool,
		pageTable:   make(map[disk.PageID]PoolIndex),
	}
}

func (p *PoolManager) kickPage() (*Page, PoolIndex, error) {
	// プールの空きを取得
	poolIndex, err := p.pool.clockSweep()
	if err != nil {
		return nil, InvalidIndex, err
	}

	// テーブルから追い出すデータを削除
	page := p.pool.getPage(poolIndex)
	delete(p.pageTable, page.GetID())

	// データが更新されていたら、ファイルに書き込む
	if page.GetUpdate() {
		err = p.fileManager.WritePageData(page.GetID(), page.GetData())
		if err != nil {
			return nil, InvalidIndex, err
		}
	}

	return page, poolIndex, nil
}

// ポインタを返すので注意
// コピーを返す方がバグが減りそうだけど、そうなると変更できない？直接プールマネージャを通して変更する方法しかない？
func (p *PoolManager) FetchPage(pageID disk.PageID) (*Page, error) {
	if pageID <= disk.InvalidID {
		return nil, errors.New("invalid page id")
	}

	// テーブルにある場合 return
	if poolIndex, ok := p.pageTable[pageID]; ok {
		page := p.pool.getPage(poolIndex)
		page.addPin()
		return page, nil
	}

	// テーブルにない場合、プール内の使って良いページを探す
	page, poolIndex, err := p.kickPage()
	if err != nil {
		return nil, err
	}

	// ファイルからデータを読み込む
	page.SetID(pageID)
	page.SetUpdate(false)
	p.fileManager.ReadPageData(pageID, page.GetData())
	page.addPin()
	p.pageTable[pageID] = poolIndex

	return page, nil
}

// pinは増えない
func (p *PoolManager) CreatePage() (disk.PageID, error) {
	// プール内の使って良いページを探す
	page, poolIndex, err := p.kickPage()
	if err != nil {
		return disk.InvalidID, err
	}

	// 新しいページを作成
	newPageID, err := p.fileManager.AllocateNewPage()
	if err != nil {
		return disk.InvalidID, err
	}
	page.reset()
	page.SetID(newPageID)
	page.SetUpdate(true)
	p.pageTable[newPageID] = poolIndex

	return newPageID, nil
}

func (p *PoolManager) Flush() error {
	// テーブルにあるデータを全てファイルに書き込む
	for pageId, poolIndex := range p.pageTable {
		page := p.pool.getPage(poolIndex)
		err := p.fileManager.WritePageData(pageId, page.GetData())
		if err != nil {
			return err
		}
		page.SetUpdate(false)
	}

	// ファイル同期
	err := p.fileManager.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (p *PoolManager) Close() error {
	// Flush
	err := p.Flush()
	if err != nil {
		return err
	}

	// ファイルクローズ
	err = p.fileManager.Close()
	if err != nil {
		return err
	}

	return nil
}
