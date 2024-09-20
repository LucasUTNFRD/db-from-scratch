package btree

type KV struct {
	Path string //filename
	//internal
	fd   int //file descriptor
	tree BTree
}

// func (db *KV) Open() error

// func (db *KV) Get(key []byte) ([]byte, bool) {
// 	return db.tree.Get(key)
// }
// func (db *KV) Set(key []byte, val []byte) error {
// 	db.tree.Insert(key, val)
// 	return updateFile(db)
// }
// func (db *KV) Del(key []byte) (bool, error) {
// 	deleted := db.tree.Delete(key)
// 	return deleted, updateFile(db)
// }

// func updateFile(db *KV) error {
// 	// 1. Write new nodes.
// 	if err := writePages(db); err != nil {
// 		return err
// 	}
// 	// 2. `fsync` to enforce the order between 1 and 3.
// 	if err := syscall.Fsync(db.fd); err != nil {
// 		return err
// 	}
// 	// 3. Update the root pointer atomically.
// 	if err := updateRoot(db); err != nil {
// 		return err
// 	}
// 	// 4. `fsync` to make everything persistent.
// 	return syscall.Fsync(db.fd)
// }
