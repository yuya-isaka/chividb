package pool

import (
	"errors"
	"sync"

	"github.com/yuya-isaka/chibidb/disk"
)

type PoolIndex uint64

type Page struct {
	id    disk.PageID
	data  []byte
	pin   uint64
	dirty bool
	lock  sync.RWMutex
}

func NewPage() *Page {
	return &Page{
		id:    disk.InvalidID,
		data:  make([]byte, disk.PageSize),
		pin:   0,
		dirty: false,
		lock:  sync.RWMutex{},
	}
}

func (p *Page) Reset() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.id = disk.InvalidID
	p.data = make([]byte, disk.PageSize)
	p.pin = 0
	p.dirty = false
}

func (p *Page) GetID() disk.PageID {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.id
}

func (p *Page) SetID(id disk.PageID) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.id = id
}

func (p *Page) GetData() []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.data
}

func (p *Page) SetData(data []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.data = data
}

func (p *Page) GetPin() uint64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.pin
}

func (p *Page) AddPin() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.pin++
}

func (p *Page) SubPin() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.pin--
}

func (p *Page) GetDirty() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.dirty
}

func (p *Page) SetDirty(dirty bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.dirty = dirty
}

// ======================================================================

type Pool struct {
	pages         []Page
	nextKickIndex PoolIndex
}

func NewPool(size int) *Pool {
	return &Pool{
		pages:         make([]Page, size),
		nextKickIndex: PoolIndex(0),
	}
}

func (p *Pool) GetPage(index PoolIndex) *Page {
	return &p.pages[index]
}

func (p *Pool) KickPool() (PoolIndex, error) {
	pageNum := len(p.pages)

	checkedPageNum := 0

	for {
		nextKickIndex := p.nextKickIndex
		page := p.GetPage(nextKickIndex)

		if page.GetPin() == 0 {
			return nextKickIndex, nil
		} else {
			checkedPageNum++
			if checkedPageNum >= pageNum {
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

func (p *PoolManager) FetchPage(id disk.PageID) (*Page, error) {
	poolIndex, ok := p.pageTable[id]
	if ok {
		return p.pool.GetPage(poolIndex), nil
	}

	poolIndex, err := p.pool.KickPool()
	if err != nil {
		return nil, err
	}

	page := p.pool.GetPage(poolIndex)

	kickPageID := page.GetID()
	delete(p.pageTable, kickPageID)

	if page.GetDirty() {
		err = p.fileManager.WritePageData(kickPageID, page.GetData())
		if err != nil {
			return nil, err
		}
	}

	page.SetID(id)
	page.SetDirty(false)
	page.AddPin()
	p.fileManager.ReadPageData(id, page.GetData())

	p.pageTable[id] = poolIndex

	return page, nil
}

func (p *PoolManager) CreatePage() (*Page, error) {
	targetPoolIndex, err := p.pool.KickPool()
	if err != nil {
		return nil, err
	}

	targetPage := p.pool.GetPage(targetPoolIndex)
	delete(p.pageTable, targetPage.GetID())

	if targetPage.GetDirty() {
		err = p.fileManager.WritePageData(targetPage.GetID(), targetPage.GetData())
		if err != nil {
			return nil, err
		}
	}

	newPageID, err := p.fileManager.AllocateNewPage()
	if err != nil {
		return nil, err
	}

	targetPage.Reset()

	targetPage.SetID(newPageID)
	targetPage.SetDirty(true)

	targetPage.AddPin()
	p.pageTable[newPageID] = targetPoolIndex

	return targetPage, nil
}

func (p *PoolManager) flush() error {
	for pageId, poolIndex := range p.pageTable {
		page := p.pool.GetPage(poolIndex)
		err := p.fileManager.WritePageData(pageId, page.GetData())
		if err != nil {
			return err
		}
		page.SetDirty(false)
	}
	err := p.fileManager.Sync()
	if err != nil {
		return err
	}

	return nil
}
