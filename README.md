# Go tiny DB
anoterh toy db implementation.

- 1st layer consists of  KV store based on B+Tree.
- 2st layer it consitts of Relational DB on top of the previous KV store.

- [ ] On-disk B+ tree implementation
  - [x] Btree struct
  - [x] Bnode struct
  - [x] Update methods
    - [x] Insert
    - [x] Delete
    - [ ] Exhaustive testing for edge cases in deletion
- [ ] Append only KV
- [ ] SQL-like language for KV.
  - [ ] interpreter
  - [ ] parser
