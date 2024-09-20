package btree

import "bytes"

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096 //OS page size for simplicity
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

type BTree struct {
	root uint64
	get  func(uint64) []byte // dereference a pointer
	new  func([]byte) uint64 // allocate a new page
	del  func(uint64)        // deallocate a page
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false // Tree is empty
	}

	node := BNode(tree.get(tree.root))
	for {
		idx, found := node.search(key)

		if node.getType() == BNODE_LEAF {
			if found {
				return node.getVal(idx), true
			}
			return nil, false // Key not found
		}

		// Internal node: move to the appropriate child
		if idx == node.getNKeys() {
			idx--
		}
		childPtr := node.getPtr(idx)
		node = BNode(tree.get(childPtr))
	}
}

// func (t *BTree) Insert(key, value []byte) {
// 	// Implement B+ tree insert logic with Copy-on-Write here
// }

func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}
	node := treeInsert(tree, tree.get(tree.root), key, val)
	// fmt.Println(node.nbytes())
	nsplit, split := nodeSplitInThree(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_INTERNAL, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

func (tree *BTree) Delete(key []byte) bool {
	if tree.root == 0 {
		return false // Tree is empty, nothing to delete
	}

	rootNode := tree.get(tree.root) //read the root from disk
	newRoot := treeDelete(tree, rootNode, key)

	// If the root has changed (due to merging or rebalancing)
	if !bytes.Equal(rootNode, newRoot) {
		tree.del(tree.root) // Delete the old root
		if newRoot.getNKeys() == 0 {
			// The tree has become empty
			tree.root = 0
		} else {
			// Update the root
			tree.root = tree.new(newRoot)
		}
	}

	return true // Key was found and deleted (or attempted to delete)
}
