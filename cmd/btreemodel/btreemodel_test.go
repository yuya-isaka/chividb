package btreemodel

import (
	"fmt"
	"strings"
	"testing"
)

func (tree *BPTree) PrintTree() {
	tree.printSubtree(tree.root, 0)
}

// printSubtree - Helper function to print a subtree from a node
func (tree *BPTree) printSubtree(node *Node, level int) {
	if node == nil {
		return
	}

	// Prepare the indentation for the current level
	indent := strings.Repeat("  ", level)

	// Print all keys at the current node
	fmt.Printf("%s[", indent)
	for i, key := range node.keys {
		fmt.Printf("%d", key)
		if i < len(node.keys)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("]")

	// If it's not a leaf, go deeper
	if !node.leaf {
		for _, child := range node.children {
			tree.printSubtree(child, level+1)
		}
	}
}

func TestInsertAndSearch(t *testing.T) {
	bpt := &BPTree{t: 3}
	keys := []int{10, 20, 5, 6, 12, 30, 7, 17}
	values := []string{"Value10", "Value20", "Value5", "Value6", "Value12", "Value30", "Value7", "Value17"}

	for i, key := range keys {
		bpt.Insert(key, values[i])
		if val := bpt.Search(key); val != values[i] {
			t.Errorf("Search failed for key %d, expected %s, got %v", key, values[i], val)
		}
	}
}

func TestTreeStructure(t *testing.T) {
	bpt := &BPTree{t: 2}
	keys := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, key := range keys {
		bpt.Insert(key, key*10)
		bpt.PrintTree()
	}

	// Specific structure checks (this will depend on your tree's logic and insertion order)
	if len(bpt.root.keys) != 1 || bpt.root.keys[0] != 4 {
		t.Errorf("Root keys incorrect, got %v", bpt.root.keys)
	}

	// More detailed checks can be added based on expected tree structure
}

// func TestDelete(t *testing.T) {
// 	bpt := &BPTree{t: 3}
// 	keys := []int{10, 20, 5, 6, 12, 30, 7, 17}
// 	values := []string{"Value10", "Value20", "Value5", "Value6", "Value12", "Value30", "Value7", "Value17"}

// 	for i, key := range keys {
// 		bpt.Insert(key, values[i])
// 	}

// 	// Delete some keys and check structure and search result
// 	deletions := []int{6, 20, 5}
// 	for _, key := range deletions {
// 		bpt.Delete(key)
// 		if val := bpt.Search(key); val != nil {
// 			t.Errorf("Key %d was not deleted properly, still found %v", key, val)
// 		}
// 	}

// 	// Check remaining keys
// 	remainingKeys := []int{10, 12, 30, 7, 17}
// 	for _, key := range remainingKeys {
// 		if val := bpt.Search(key); val == nil {
// 			t.Errorf("Key %d not found after deletions, but it should exist", key)
// 		}
// 	}
// }
