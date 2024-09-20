package btree

import (
	"bytes"
	"fmt"
	"testing"
	"unsafe"
)

type TestTree struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

//TODO implement Testing

func newTestTree(t *testing.T) *TestTree {
	tt := &TestTree{
		ref:   make(map[string]string),
		pages: make(map[uint64]BNode),
	}
	tt.tree = BTree{
		get: func(ptr uint64) []byte {
			page, ok := tt.pages[ptr]
			if !ok {
				t.Fatalf("Page %d not found", ptr)
			}
			return page
		},
		new: func(page []byte) uint64 {
			ptr := uint64(uintptr(unsafe.Pointer(&page[0])))
			if _, exists := tt.pages[ptr]; exists {
				t.Fatalf("Page %d already exists", ptr)
			}
			tt.pages[ptr] = page
			return ptr
		},
		del: func(ptr uint64) {
			if _, ok := tt.pages[ptr]; !ok {
				t.Fatalf("Deleting non-existent page %d", ptr)
			}
			delete(tt.pages, ptr)
		},
	}
	return tt
}

func (tt *TestTree) check(t *testing.T) {
	// - It first checks if the tree is empty (root is 0).
	//   - If the tree is empty, it ensures that the reference map is also empty.
	//   - If the tree is empty but the reference isn't, it reports an error.
	if tt.tree.root == 0 {
		if len(tt.ref) != 0 {
			t.Fatal("Tree is empty but reference is not")
		}
		return
	}

	// Check if the B-tree structure is valid
	tt.checkNode(t, tt.tree.root, nil, nil, 0)

	// Check if all keys in the reference are in the tree
	for key, expectedValue := range tt.ref {
		value, ok := tt.tree.Get([]byte(key))
		if !ok {
			t.Errorf("Key %s not found in tree", key)
		} else if string(value) != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}
}

// It recursively checks the entire tree structure.
func (tt *TestTree) checkNode(t *testing.T, ptr uint64, min, max []byte, depth int) {
	node := BNode(tt.tree.get(ptr))

	// Check node size
	if node.nbytes() > BTREE_PAGE_SIZE {
		t.Errorf("Node size %d exceeds page size %d", node.nbytes(), BTREE_PAGE_SIZE)
	}

	nkeys := node.getNKeys()

	// Check key order and bounds
	var prevKey []byte
	for i := uint16(0); i < nkeys; i++ {
		key := node.getKey(i)

		if min != nil && bytes.Compare(key, min) < 0 {
			t.Errorf("Key %s is less than lower bound %s", string(key), string(min))
		}
		if max != nil && bytes.Compare(key, max) >= 0 {
			t.Errorf("Key %s is greater than or equal to upper bound %s", string(key), string(max))
		}

		if prevKey != nil && bytes.Compare(prevKey, key) >= 0 {
			t.Errorf("Keys not in ascending order: %s >= %s", string(prevKey), string(key))
		}
		prevKey = key

		if node.getType() == BNODE_INTERNAL {
			tt.checkNode(t, node.getPtr(i), min, key, depth+1)
			min = key
		}
	}

	if node.getType() == BNODE_INTERNAL {
		tt.checkNode(t, node.getPtr(nkeys), min, max, depth+1)
	}
}

func (tt *TestTree) insert(t *testing.T, key, value string) {
	tt.tree.Insert([]byte(key), []byte(value))
	tt.ref[key] = value
	tt.check(t)
}

func (tt *TestTree) delete(t *testing.T, key string) {
	tt.tree.Delete([]byte(key))
	delete(tt.ref, key)
	tt.check(t)
}

func TestBTreeBasic(t *testing.T) {
	t.Log("Tests basic operations like insertion, update, and deletion.")
	tt := newTestTree(t)

	// Test insertion
	tt.insert(t, "key1", "val1")
	tt.insert(t, "key2", "val2")
	tt.insert(t, "key3", "val3")

	// Test update
	tt.insert(t, "key2", "val2_updated")

	// Test deletion
	tt.delete(t, "key1")

	// Test non-existent key
	_, ok := tt.tree.Get([]byte("non_existent"))
	if ok {
		t.Error("Found non-existent key")
	}
}

func TestBTreeDeletionEdgeCases(t *testing.T) {
	t.Log("Test for edge cases in deletion")
	tt := newTestTree(t)

	// Test deleting from an empty tree
	tt.delete(t, "non_existent")

	// // Test deleting the only key in the tree
	tt.insert(t, "only_key", "only_value")
	tt.delete(t, "only_key")

	// // Test deleting the first key in a node
	tt.insert(t, "a", "1")
	tt.insert(t, "b", "2")
	tt.insert(t, "c", "3")
	tt.delete(t, "a")

	// Test deleting a key that causes nodes to merge
	for i := 0; i < 100; i++ {
		tt.insert(t, fmt.Sprintf("key%03d", i), fmt.Sprintf("val%03d", i))
	}
	for i := 0; i < 50; i++ {
		tt.delete(t, fmt.Sprintf("key%03d", i))
	}
}
