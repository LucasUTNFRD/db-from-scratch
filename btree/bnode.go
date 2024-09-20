package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// | type | nkeys | pointers | offsets | key-values | unused |
// | 2B | 2B | nkeys * 8B | nkeys * 2B |

// | klen | vlen | key | val |
// | 2B | 2B | ... | ... |
type BNode []byte

const (
	BNODE_INTERNAL = 1
	BNODE_LEAF     = 2
)

// helper functions

// For reading Node Data: Helper functions are used to interpret parts of the byte slice:

// getType(): Reads the node type (leaf or internal)
// getNKeys(): Reads the number of keys in the node
// getPtr(idx): Reads a pointer to a child node at a specific index
// getKey(idx): Reads a key at a specific index
// getVal(idx): Reads a value at a specific index

// Header operations
func (node BNode) getType() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) setType(typ uint16) {
	binary.LittleEndian.PutUint16(node[0:2], typ)
}

func (node BNode) getNKeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setNKeys(nkeys uint16) {
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// The setHeader function is used to initialize or update the header information of a B-tree node
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	node.setType(btype)
	node.setNKeys(nkeys)
}

// Child pointers
func (node BNode) getPtr(idx uint16) uint64 {
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// KV offsets and pairs
func offsetPos(node BNode, idx uint16) uint16 {
	return HEADER + 8*node.getNKeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.getNKeys() {
		panic("index out of range")
	}
	return HEADER + 8*node.getNKeys() + 2*node.getNKeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	// if idx >= node.getNKeys() {
	// 	panic("index out of range")
	// }
	nkeys := node.getNKeys()
	if idx >= nkeys {
		panic(fmt.Sprintf("index out of range: idx=%d, nkeys=%d, node type=%d", idx, nkeys, node.getType()))
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	// return node[pos+4 : pos+4+uint32(klen)]
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.getNKeys())
}

//TODO implement B+tree needed operation for Insert method
//  leafInsert updates a leaf node.
//  nodeReplaceKidN updates an internal node.
//  nodeSplit splits an oversized node.

// search returns the index where the key should be inserted and whether the key was found.
// If the key is found, the returned index is the position of the existing key.
// If the key is not found, the returned index is where the new key should be inserted.

func (node BNode) search(key []byte) (uint16, bool) {
	low, high := uint16(0), node.getNKeys()-1
	var mid uint16

	for low <= high {
		mid = (low + high) / 2
		cmp := bytes.Compare(key, node.getKey(mid))

		if cmp == 0 {
			return mid, true // Key found
		} else if cmp > 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return low, false // Key not found, return insertion point
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := make([]byte, 2*BTREE_PAGE_SIZE)
	newNode := BNode(new)
	idx, found := node.search(key) //implement search usign BS.

	switch node.getType() {
	case BNODE_LEAF:
		if found {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx, key, val)
		}
	case BNODE_INTERNAL:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("Bad node!")
	}
	return newNode
}

// copy multiple KVs into the posistoin from the old node
func nodeAppendRange(new, old BNode, dstStart, srcStart, n uint16) {
	for i := uint16(0); i < n; i++ {
		nodeAppendKV(new, dstStart+i, old.getPtr(srcStart+i), old.getKey(srcStart+i), old.getVal(srcStart+i))
	}
}

func nodeAppendKV(node BNode, idx uint16, ptr uint64, key, val []byte) {
	node.setPtr(idx, ptr)
	kv := node.kvPos(idx)
	binary.LittleEndian.PutUint16(node[kv:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[kv+2:], uint16(len(val)))
	copy(node[kv+4:], key)
	copy(node[kv+4+uint16(len(key)):], val)
	node.setOffset(idx+1, node.getOffset(idx)+4+uint16(len(key))+uint16(len(val)))
	// node.setNKeys(node.getNKeys() + 1)
}

// add a new key to a leaf node
func leafInsert(new, old BNode, idx uint16, key, val []byte) {
	new.setHeader(BNODE_LEAF, old.getNKeys()+1) //  setup the header
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.getNKeys()-idx)
}

func leafUpdate(new, old BNode, idx uint16, key, val []byte) {
	// Set the header for the new node
	new.setHeader(BNODE_LEAF, old.getNKeys())

	// Copy all key-value pairs before the update index
	nodeAppendRange(new, old, 0, 0, idx)

	// Update the key-value pair at the given index
	nodeAppendKV(new, idx, 0, key, val)

	// Copy all key-value pairs after the update index
	nodeAppendRange(new, old, idx+1, idx+1, old.getNKeys()-idx-1)
}

// replace a link with one or multiple links
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_INTERNAL, old.getNKeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
		// 				  ^position 	^pointer		^key 			^val
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.getNKeys()-(idx+1))
}

func nodeSplitInTwo(left, right, old BNode) {
	// Calculate the midpoint where the split should occur
	splitPoint := len(old) / 2

	// Copy the first half of the node to the left node
	copy(left, old[:splitPoint])

	// Copy the second half of the node to the right node
	copy(right, old[splitPoint:])

	// Optionally trim the excess bytes if needed
	left = left[:splitPoint]
	right = right[:len(old)-splitPoint]
}

func nodeSplitInThree(old BNode) (nsplit uint16, splitSlice [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplitInTwo(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplitInTwo(leftleft, middle, left)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

func nodeInsert(tree *BTree, new, node BNode, idx uint16, key, val []byte) {
	kptr := node.getPtr(idx)
	// recursive insertion to the kid node
	knode := treeInsert(tree, tree.get(kptr), key, val)
	// split the result
	nsplit, split := nodeSplitInThree(knode)
	// deallocate the kid node
	tree.del(kptr)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {

	new := make([]byte, 2*BTREE_PAGE_SIZE)
	newNode := BNode(new)
	idx, found := node.search(key)

	switch node.getType() {
	case BNODE_LEAF:
		if !found {
			return BNode{}
		} else {
			leafDelete(newNode, node, idx)
		}
	case BNODE_INTERNAL:
		nodeDelete(tree, node, idx, key)
	default:
		panic("Bad node!")
	}

	return newNode
}

// should the updated kid be merged with a sibling?
func shouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}
	if idx+1 < node.getNKeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}
	return 0, BNode{}
}

// delete a key from an internal node; partf of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.getNKeys() == 0:
		// assert(node.getNKeys() == 1 && idx == 0) // 1 empty child but no sibling
		if node.getNKeys() != 1 || updated.getNKeys() != 0 {
			panic("one empty child but no sibling")
		}
		new.setHeader(BNODE_INTERNAL, 0) // the parent becomes empty too
	case mergeDir == 0 && updated.getNKeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.getNKeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.getNKeys()-idx-1)
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.getType(), left.getNKeys()+right.getNKeys())
	nodeAppendRange(new, left, 0, 0, left.getNKeys())
	nodeAppendRange(new, right, left.getNKeys(), 0, right.getNKeys())
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16, ptr uint64, key []byte,
) {
	new.setHeader(BNODE_INTERNAL, old.getNKeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.getNKeys()-idx-2)
}
