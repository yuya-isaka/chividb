package btreemodel

// Node - B+ tree node
type Node struct {
	keys     []int
	values   []interface{}
	children []*Node
	leaf     bool
}

// BPTree - B+ tree structure
type BPTree struct {
	root *Node
	t    int // Maximum number of children a node can have
}

func (tree *BPTree) Search(key int) interface{} {
	current := tree.root
	for current != nil {
		// ノード内でのキーの位置を探す
		i := 0
		for i < len(current.keys) && key > current.keys[i] {
			i++
		}

		if i < len(current.keys) && key == current.keys[i] {
			if current.leaf {
				return current.values[i]
			}
		}

		if current.leaf {
			return nil
		}

		current = current.children[i]
	}
	return nil
}

func (tree *BPTree) Insert(key int, value interface{}) {
	root := tree.root
	if root == nil {
		tree.root = &Node{keys: []int{key}, values: []interface{}{value}, leaf: true}
		return
	}

	// ルートが分割を必要とするかどうかをチェック
	if len(root.keys) >= 2*tree.t-1 {
		newRoot := &Node{children: []*Node{root}, leaf: false}
		tree.splitChild(newRoot, 0)
		tree.root = newRoot
	}

	tree.insertNonFull(tree.root, key, value)
}

// ノードが完全に満たされていない場合の挿入
func (tree *BPTree) insertNonFull(node *Node, key int, value interface{}) {
	i := len(node.keys) - 1
	if node.leaf {
		// 葉ノードでの挿入位置を見つける
		// 対象keyの左のインデックス
		for i >= 0 && node.keys[i] > key {
			i--
		}
		// キーと値を挿入
		node.keys = append(node.keys[:i+1], append([]int{key}, node.keys[i+1:]...)...) // インデックスまで: キー :決めたインデックスの次
		node.values = append(node.values[:i+1], append([]interface{}{value}, node.values[i+1:]...)...)
	} else {
		// 葉ノードでない場合は、子ノードを探索
		for i >= 0 && node.keys[i] > key {
			i--
		}
		i++
		if len(node.children[i].keys) >= 2*tree.t-1 {
			// この時のスプリットインデックスはi
			tree.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		}
		tree.insertNonFull(node.children[i], key, value)
	}
}

// 子ノードの分割
func (tree *BPTree) splitChild(parent *Node, index int) {
	// 子ノード
	node := parent.children[index]
	// たとえば現在のノードが3つのキーを持っている場合、中央は1
	midIndex := len(node.keys) / 2
	midKey := node.keys[midIndex]

	// 新しいノードの作成
	// 中央の次の位置からのキーを新しいノードに移動
	// ここで作るノードは、元のノードの状態を引き継ぐ（つまり葉なら、葉のまま）
	newNode := &Node{keys: append([]int{}, node.keys[midIndex+1:]...), leaf: node.leaf}

	if node.leaf {
		// 分割対象のノードが葉っぱなら、値を新しいノードに移動させる
		newNode.values = append([]interface{}{}, node.values[midIndex+1:]...)

		// 現在のノードには、中央インデックス-1までのキーと値を残す
		node.keys = node.keys[:midIndex]
		// 同じく値も
		node.values = node.values[:midIndex]
	} else {
		// 枝の場合、下の子ノードのインデックスを考慮しないといけない
		// 今回は、midIndex+1の場所を新しいノードの子供にする
		newNode.children = append([]*Node{}, node.children[midIndex+1:]...)
		// 現在のキーの更新
		node.keys = node.keys[:midIndex]
		// 子ノードも更新
		node.children = node.children[:midIndex+1]
	}

	// 親ノードに中央キーを挿入
	// 入れる場所は、indexの位置
	parent.keys = append(parent.keys[:index], append([]int{midKey}, parent.keys[index:]...)...)

	// 子ノードを入れる場所は、中央になったキーの左と右に入れる
	// だから、今回だと0の位置に中央ノードが入っている
	// それの場合、子ノードのそのインデックスまでを左、新しいノード、そのインデックス＋1を右に入れる
	parent.children = append(parent.children[:index+1], append([]*Node{newNode}, parent.children[index+1:]...)...)
}

// func (tree *BPTree) Delete(key int) {
// 	tree.delete(tree.root, key)
// }

// func (tree *BPTree) delete(node *Node, key int) bool {
// 	if node == nil {
// 		return false
// 	}

// 	// キーの位置を探す
// 	i := 0
// 	for i < len(node.keys) && key > node.keys[i] {
// 		i++
// 	}

// 	// 葉でキーを削除
// 	if node.leaf {
// 		if i < len(node.keys) && node.keys[i] == key {
// 			node.keys = append(node.keys[:i], node.keys[i+1:]...)
// 			node.values = append(node.values[:i], node.values[i+1:]...)
// 			return len(node.keys) < tree.t-1
// 		}
// 		return false
// 	}

// 	// 内部ノードでキーを削除
// 	mustDelete := false
// 	if i < len(node.keys) && node.keys[i] == key {
// 		mustDelete = true
// 		i++
// 	}
// 	underflow := tree.delete(node.children[i], key)

// 	if mustDelete && len(node.children[i].keys) > 0 {
// 		successor := tree.getSmallest(node.children[i])
// 		node.keys[i-1] = successor
// 	}

// 	// 子ノードがアンダーフローの場合の処理
// 	if underflow {
// 		if i > 0 && len(node.children[i-1].keys) > tree.t-1 {
// 			// 左の兄弟から借りる
// 			tree.borrowFromLeft(node, i)
// 		} else if i < len(node.children)-1 && len(node.children[i+1].keys) > tree.t-1 {
// 			// 右の兄弟から借りる
// 			tree.borrowFromRight(node, i)
// 		} else {
// 			// 統合
// 			if i > 0 {
// 				tree.merge(node, i-1)
// 			} else {
// 				tree.merge(node, i)
// 			}
// 		}
// 		return len(node.keys) < tree.t-1
// 	}

// 		return false
// 	}

// func (tree *BPTree) getSmallest(node *Node) int {
// 	if node.leaf {
// 		return node.keys[0]
// 	}
// 	return tree.getSmallest(node.children[0])
// }

// func (tree *BPTree) borrowFromLeft(parent *Node, index int) {
// 	current := parent.children[index]
// 	leftSibling := parent.children[index-1]

// 	// 左兄弟から最後のキーを現在のノードの最初のキーとして移動
// 	current.keys = append([]int{parent.keys[index-1]}, current.keys...)
// 	current.values = append([]interface{}{leftSibling.values[len(leftSibling.values)-1]}, current.values...)
// 	parent.keys[index-1] = leftSibling.keys[len(leftSibling.keys)-1]

// 	// 必要であれば子ノードも移動
// 	if !current.leaf {
// 		current.children = append([]*Node{leftSibling.children[len(leftSibling.children)-1]}, current.children...)
// 		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
// 	}

// 	// 左兄弟からキーを削除
// 	leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
// 	leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]
// }

// func (tree *BPTree) borrowFromRight(parent *Node, index int) {
// 	current := parent.children[index]
// 	rightSibling := parent.children[index+1]

// 	// 右兄弟から最初のキーを現在のノードの最後のキーとして移動
// 	current.keys = append(current.keys, parent.keys[index])
// 	current.values = append(current.values, rightSibling.values[0])
// 	parent.keys[index] = rightSibling.keys[0]

// 	// 必要であれば子ノードも移動
// 	if !current.leaf {
// 		current.children = append(current.children, rightSibling.children[0])
// 		rightSibling.children = rightSibling.children[1:]
// 	}

// 	// 右兄弟からキーを削除
// 	rightSibling.keys = rightSibling.keys[1:]
// 	rightSibling.values = rightSibling.values[1:]
// }

// func (tree *BPTree) merge(parent *Node, index int) {
// 	left := parent.children[index]
// 	right := parent.children[index+1]

// 	// 親ノードからキーを取得し、左ノードに追加
// 	left.keys = append(left.keys, parent.keys[index])
// 	left.values = append(left.values, right.values...)
// 	left.keys = append(left.keys, right.keys...)
// 	if !left.leaf {
// 		left.children = append(left.children, right.children...)
// 	}

// 	// 親ノードから右ノードを削除
// 	parent.keys = append(parent.keys[:index], parent.keys[index+1:]...)
// 	parent.children = append(parent.children[:index+1], parent.children[index+2:]...)

// 	// 右ノードのリソースを解放する処理が必要であればここに記述
// }
