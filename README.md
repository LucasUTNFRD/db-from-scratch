# Go tiny DB
anoterh toy db implementation.

- 1st layer consists of  KV store based on B+Tree.
- 2st layer it consitts of Relational DB on top of the previous KV store.

- [ ] On-disk B+ tree implementation
  - [ ] Btree struct
  - [ ] Bnode struct
  - [ ] Update methods
    - [x] Insert
    - [ ] Delete
- [ ] Append only KV
- [ ] SQL-like language for KV.
  - [ ] interpreter
  - [ ] parser
