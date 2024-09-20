package btree

import "fmt"

const (
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096 //OS page size for simplicity
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

type BTree struct {
	root uint64
	get  func(uint64) []byte // Read a page from disk
	new  func([]byte) uint64 // Append a new page to disk
	del  func(uint64)        // Delete a page from disk (optional)
}

func (t *BTree) Get(key []byte) ([]byte, bool) {
	// Implement B+ tree search logic here
	return nil, false
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
	fmt.Println(node.nbytes())
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

func (t *BTree) Delete(key []byte) bool {
	// Implement B+ tree delete logic with Copy-on-Write here
	return false
}
