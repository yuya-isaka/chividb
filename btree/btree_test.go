package btree

import (
	"os"
	"testing"

	"github.com/yuya-isaka/chibidb/pool"
)

func TestBTreeInsertAndSearch(t *testing.T) {
	testFile := "testdata"
	poolManager, err := pool.NewPoolManager("testdata", 100)
	if err != nil {
		t.Fatalf("Failed to create pool manager: %v", err)
	}
	defer poolManager.Close()
	defer os.Remove(testFile)

	// BTreeの新規作成
	btree, err := NewBTree(poolManager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// テストデータの挿入
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	for i, key := range keys {
		err := btree.Insert(key, values[i])
		if err != nil {
			t.Errorf("Failed to insert key %s: %v", key, err)
		}
	}

	// 挿入したデータの検索と確認
	for i, key := range keys {
		got, err := btree.Search(key)
		if err != nil {
			t.Errorf("Failed to search key %s: %v", key, err)
		}
		if string(got) != string(values[i]) {
			t.Errorf("Expected value %s, got %s", values[i], got)
		}
	}
}
