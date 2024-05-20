package page

import (
	"encoding/binary"
	"log"

	"github.com/yuya-isaka/chibidb/bsearch"
	"github.com/yuya-isaka/chibidb/disk"
	"github.com/yuya-isaka/chibidb/util"
)

const (
	NoneNodeType   string = "        " // 葉ノード、8 bytes
	LeafNodeType   string = "LEAF    " // 葉ノード、8 bytes
	BranchNodeType string = "BRANCH  " // 枝ノード、8 bytes
	MaxPairSize    uint16 = 4064
)

type Pair struct {
	Key   []byte
	Value []byte
}

func NewPair(key []byte, value []byte) *Pair {
	return &Pair{
		Key:   key,
		Value: value,
	}
}

// ==============================================================================

// ストレージの1ページを表す構造体
type Page struct {
	PageID   disk.PageID // ページの一意なID
	pageData []byte      // ページのデータ内容
	Counter  uint        // ページの参照数（Clock-Sweepアルゴリズムで使用）
	Flag     bool        // ページの更新フラグ
}

func NewPage() *Page {
	return &Page{
		PageID:   disk.PageID(-1),
		pageData: make([]byte, 4096),
		Counter:  0,
		Flag:     false,
	}
}

func (p *Page) ResetPage() {
	p.PageID = disk.PageID(-1)
	p.Counter = 0
	p.Flag = false
}

func (p *Page) ResetPageData() {
	p.SetNodeType(NoneNodeType)
	p.SetPrevID(disk.PageID(-1))
	p.SetNextID(disk.PageID(-1))
	p.SetPointersNum(0)
	p.SetFreeOffset(4096)
	p.SetBody(make([]byte, 4068))
}

func (p *Page) GetAllData() []byte {
	return p.pageData[0:4096]
}

func (p *Page) GetNodeType() string {
	return string(p.pageData[0:8])
}

func (p *Page) SetNodeType(nt string) {
	p.SetData(0, 8, []byte(nt))
}

func (p *Page) GetPrevID() disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(p.pageData[8:16]))
}

func (p *Page) SetPrevID(prevID disk.PageID) {
	p.SetData(8, 16, util.PageIDTo8Bytes(prevID))
}

func (p *Page) GetNextID() disk.PageID {
	return disk.PageID(binary.LittleEndian.Uint64(p.pageData[16:24]))
}

func (p *Page) SetNextID(nextID disk.PageID) {
	p.SetData(16, 24, util.PageIDTo8Bytes(nextID))
}

func (p *Page) GetPointersNum() uint16 {
	return binary.LittleEndian.Uint16(p.pageData[24:26])
}

func (p *Page) SetPointersNum(numPtrs uint16) {
	p.SetData(24, 26, util.Uint16To2Bytes(numPtrs))
}

func (p *Page) GetFreeOffset() uint16 {
	return binary.LittleEndian.Uint16(p.pageData[26:28])
}

func (p *Page) SetFreeOffset(freeOffset uint16) {
	p.SetData(26, 28, util.Uint16To2Bytes(freeOffset))
}

func (p *Page) GetBody() []byte {
	return p.pageData[28:]
}

func (p *Page) SetBody(data []byte) {
	p.SetData(28, 4096, data)
}

func (p *Page) GetPair(index uint16) *Pair {
	offset := binary.LittleEndian.Uint16(p.pageData[28+index*4 : 28+index*4+2])
	length := binary.LittleEndian.Uint16(p.pageData[28+index*4+2 : 28+index*4+4])

	// Keyの先頭2バイトにはKeyの長さが格納されている
	keyStartOffset := offset + 2
	keyEndOffset := keyStartOffset + binary.LittleEndian.Uint16(p.pageData[offset:offset+2])

	return NewPair(p.pageData[keyStartOffset:keyEndOffset], p.pageData[keyEndOffset:offset+length])
}

func (p *Page) GetKey(index uint16) []byte {
	return p.GetPair(index).Key
}

func (p *Page) GetValue(index uint16) []byte {
	return p.GetPair(index).Value
}

// 葉ノードにキーと値のペアを挿入する関数
func (p *Page) InsertPair(index uint16, pair *Pair) {
	// ペアを挿入する場所が最後の位置より前の場合、ペアをシフトして空きスペースを作る
	if index < p.GetPointersNum() {
		p.shiftPairsRight(index)
	}
	p.insertPair(index, pair)
}

// 指定されたインデックスから右にペアをシフトする関数
func (p *Page) shiftPairsRight(startIndex uint16) {
	lastIndex := p.GetPointersNum() - 1
	// 最後のペアを一つ後ろに移動
	p.InsertPair(lastIndex+1, p.GetPair(lastIndex))
	for j := lastIndex; j > startIndex; j-- {
		// 右に一つずつペアをシフト
		p.updatePair(j, p.GetPair(j-1))
	}
}

func (p *Page) shiftPairsLeft(startIndex uint16) {
	for j := startIndex; j < p.GetPointersNum(); j++ {
		if j+1 < p.GetPointersNum() {
			// 左に一つずつペアをシフト
			p.updatePair(j, p.GetPair(j+1))
		} else {
			// 最後のペアの場合、データをクリア
			p.SetData(28+j*4, 28+j*4+2, util.Uint16To2Bytes(0))
			p.SetData(28+j*4+2, 28+j*4+4, util.Uint16To2Bytes(0))
		}
	}
}

func (p *Page) insertPair(index uint16, pair *Pair) {
	// Keyの長さを格納する2バイトも含める
	pairSize := uint16(len(pair.Key) + len(pair.Value) + 2)
	// ポインタのサイズも含める
	if p.GetFreeNum() < pairSize+4 {
		// 手抜き
		log.Panicf("no free space: got %d, want %d", p.GetFreeOffset(), pairSize)
		return
	}

	// もしindexの場所に既にペアが存在している場合、Panic
	if index < p.GetPointersNum() {
		log.Panicf("pair already exists at index %d", index)
		return
	}

	// 1. スロット数とフリーオフセット更新
	p.SetPointersNum(p.GetPointersNum() + 1)
	p.SetFreeOffset(p.GetFreeOffset() - pairSize)

	// 2. スロットポインタ更新
	// offset
	p.SetData(28+index*4, 28+index*4+2, util.Uint16To2Bytes(p.GetFreeOffset()))
	// length
	p.SetData(28+index*4+2, 28+index*4+4, util.Uint16To2Bytes(pairSize))

	// 3. スロットボディ更新
	// すでにFreeOffsetは更新されているので、その位置にペアを挿入する
	// keyLength 2byte
	p.SetData(p.GetFreeOffset(), p.GetFreeOffset()+2, util.Uint16To2Bytes(uint16(len(pair.Key))))
	// key
	p.SetData(p.GetFreeOffset()+2, p.GetFreeOffset()+2+uint16(len(pair.Key)), pair.Key)
	// value
	p.SetData(p.GetFreeOffset()+2+uint16(len(pair.Key)), p.GetFreeOffset()+pairSize, pair.Value)
}

func (p *Page) updatePair(index uint16, pair *Pair) {
	// Keyの長さを格納する2バイトも含める
	pairSize := uint16(len(pair.Key) + len(pair.Value) + 2)
	// ポインタのサイズも含める
	if p.GetFreeNum() < pairSize+4 {
		log.Panicf("no free space: got %d, want %d", p.GetFreeOffset(), pairSize)
		return
	}

	if index >= p.GetPointersNum() {
		log.Panicf("pair does not exist at index %d", index)
		return
	}

	// 1. スロットポインタ更新
	// offset
	p.SetData(28+index*4, 28+index*4+2, util.Uint16To2Bytes(p.GetFreeOffset()))
	// length
	p.SetData(28+index*4+2, 28+index*4+4, util.Uint16To2Bytes(pairSize))

	// 2. スロットボディ更新
	offset := binary.LittleEndian.Uint16(p.pageData[28+index*4 : 28+index*4+2])
	length := binary.LittleEndian.Uint16(p.pageData[28+index*4+2 : 28+index*4+4])
	p.SetData(offset, offset+2, util.Uint16To2Bytes(uint16(len(pair.Key))))
	p.SetData(offset+2, offset+2+uint16(len(pair.Key)), pair.Key)
	p.SetData(offset+2+uint16(len(pair.Key)), offset+length, pair.Value)
}

func (p *Page) DeletePair(index uint16) {
	if index >= p.GetPointersNum() {
		log.Panicf("pair does not exist at index %d", index)
		return
	}

	// 1. スロット数更新
	p.SetPointersNum(p.GetPointersNum() - 1)

	// 2. フリーオフセット更新
	// 現状何もしない
	// オフセットは増やしていく、コンパクションは、後で考える
	// length := binary.LittleEndian.Uint16(p.pageData[28+index*4+2 : 28+index*4+4])
	// p.SetFreeOffset(p.GetFreeOffset() + length)

	// 3. スロットポインタ更新
	p.shiftPairsLeft(index)

	// 4. スロットボディ更新
	// 何もしない
	// 論理削除（物理的にはデータは残るが、まあよし）
}

func (p *Page) GetFreeNum() uint16 {
	return p.GetFreeOffset() - 28 - p.GetPointersNum()*4
}

func (p *Page) GetLimitPairSize() uint16 {
	return p.GetFreeNum() / 2
}

func (p *Page) SearchKey(key []byte) (uint16, bool) {
	return bsearch.BinarySearch(p.GetPointersNum(), func(i uint16) util.Ordering {
		targetKey := p.GetPair(i).Key
		return util.CompareByteSlice(targetKey, key)
	})
}

// ===================================================================================================

func (p *Page) SetData(start uint16, end uint16, data []byte) {
	length := uint16(len(data))
	if end-start != length {
		log.Panicf("設定しようとしたデータサイズが不正です。データサイズ: %d バイト, 許容される最大サイズ: %d バイト", length, 4096-start)
		return
	}

	if end > 4096 {
		log.Panicf("指定されたデータ範囲がページサイズを超えています。開始位置: %d, 終了位置: %d, ページサイズ: %d バイト", start, end, len(p.pageData))
		return
	}

	p.Flag = true
	copy(p.pageData[start:start+length], data)
}
