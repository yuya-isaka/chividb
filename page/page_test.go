package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yuya-isaka/chibidb/disk"
)

func TestNewPage(t *testing.T) {
	p := NewPage()
	assert.NotNil(t, p)
	assert.Equal(t, disk.PageID(-1), p.PageID)
	assert.Equal(t, make([]byte, 4096), p.pageData)
	assert.Equal(t, uint(0), p.Counter)
	assert.False(t, p.Flag)
}

func TestSetDataAndGetAllData(t *testing.T) {
	p := NewPage()
	data := []byte("node_type")
	p.SetData(0, 9, data)
	assert.Equal(t, data, p.pageData[0:9])
	assert.Equal(t, data, p.GetAllData()[0:9])
}

func TestSetPairAndGetPair(t *testing.T) {
	p := NewPage()
	p.SetFreeOffset(4096)
	key := []byte("key")
	value := []byte("value")
	pair := NewPair(key, value)
	p.InsertPair(0, pair)

	retrievedPair := p.GetPair(0)
	assert.Equal(t, key, retrievedPair.Key)
	assert.Equal(t, value, retrievedPair.Value)
}

func TestSetDataOutOfRange(t *testing.T) {
	p := NewPage()
	data := []byte("this is a test that exceeds the boundaries of the page data allowed")

	assert.Panics(t, func() {
		p.SetData(0, uint16(len(p.pageData)+1), data)
	}, "The code did not panic when it should have")
}

func TestSetPairNoFreeSpace(t *testing.T) {
	p := NewPage()
	key := make([]byte, 3000)
	value := make([]byte, 1100)
	pair := NewPair(key, value)

	assert.Panics(t, func() {
		p.InsertPair(0, pair)
	}, "The code did not panic when the pair was too large to fit in the free space")
}

func TestErrorHandling(t *testing.T) {
	p := NewPage()
	data := make([]byte, 4100) // deliberately too large
	assert.PanicsWithValue(t, "指定されたデータ範囲がページサイズを超えています。開始位置: 0, 終了位置: 4100, ページサイズ: 4096 バイト", func() {
		p.SetData(0, 4100, data)
	})
}

func TestSearchKey(t *testing.T) {
	p := NewPage()
	p.ResetPageData()
	key1 := []byte("apple")
	key2 := []byte("banana")
	pair1 := NewPair(key1, []byte("red"))
	pair2 := NewPair(key2, []byte("yellow"))
	p.InsertPair(0, pair1)
	p.InsertPair(1, pair2)

	index, found := p.SearchKey([]byte("banana"))
	assert.True(t, found)
	assert.Equal(t, uint16(1), index)

	_, found = p.SearchKey([]byte("cherry"))
	assert.False(t, found)
}
