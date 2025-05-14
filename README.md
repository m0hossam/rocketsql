## RocketSQL 🚀

This is a _**work-in-progress**_ database engine built from scratch inspired by SQLite, Database Design and Implementation by Edward Sciore, CMU's Intro to Database Systems, and various other sources.

## Supported SQL

### Queries:

```sql
SELECT * FROM t
SELECT a, b, c FROM t
SELECT a, b, c FROM t WHERE a = 5
SELECT * FROM x, y WHERE x_id = y_id
```

### DDL:

```sql
CREATE TABLE t (c1 INT, c2 FLOAT, c3 VARCHAR(32))
```

### DML:

```sql
INSERT INTO t (c1, c2, c3) VALUES (1, 12.55, 'Hello, world')
INSERT INTO t (c3, c1, c2) VALUES ('Look at you, hacker', 2, 15.88)
INSERT INTO t VALUES (3, 99.99, 'Aloha')
```

```sql
DELETE FROM t
DELETE FROM t WHERE a = 5
```

```sql
UPDATE t SET a = 15
UPDATE t SET a = 15 WHERE b = 'Guava'
```

Notes:
- All operations are done through B-Tree algorithms.
- Due to the balanced nature of the B-Tree data structure, the records are sorted in ascending order according to their primary keys, this allows for binary search retrieval.
- Insertion sometimes causes the B-Tree to split nodes, rearrange the records among the nodes (i.e. load-balancing) and increase the tree’s depth by one.
- Deletion causes the removal of a record from its containing leaf page only, there might exist a key pointing to the deleted record in one of the interior pages. Deletion never does load-balancing, this could be changed in future implementations.
- Updates consist of a delete operation followed by an insert operation.

## The Database

The database consists of a single file on disk. The file is divided into fixed-size pages. The page format is depicted below:

![PageFormat](https://github.com/user-attachments/assets/c71c9897-ceac-4aa3-bc39-ca9bfbeb0883)

Internally, each database table is represented as a single B-Tree. Each B-Tree node is represented on disk as a page. To access the records of any table, we would only need to know the page number of the table’s root page (root node).

## Supported Datatypes

The supported datatypes are:

![DataTypes](https://github.com/user-attachments/assets/02451817-6d07-441c-b8bd-2e47e5557ff3)

The VARCHAR datatype can be supported due to the variable-length
record format adopted by the storage engine. The following diagram
depicts the format of records/cells inside the database file:

![CellFormat](https://github.com/user-attachments/assets/a268623c-3eec-4b02-9dbf-bb989a86aac0)

Note: Key Size and Data Size are 2-byte fields. Each CHAR/VARCHAR
field is preceded by a 2-byte size field.

## Current Issues

B-Tree:
- The delete operation does not remove interior B-Tree keys or decrease the depth of the B-Tree. This is space wasteful.
- The delete operation does not delete an entire leaf page when it becomes completely empty. It should be truncated and added to a list of free pages to be reused later for future B-Trees.
- The algorithm that allocates free space for new cells is not well tested, there may exist corner cases that break the algorithm.
- The algorithm that creates and manages free blocks upon cell deletion is not tested enough.

SQL:
- Query planning not implemented, semantics are mostly not checked
- Query optimization not implemented
- Passing integer parameters to float fields will result in an error
- Semicolon termination not implemented, cannot do multi-line statements
- `NULL` handling is not implemented

## Unsupported Features

SQL:
- Dropping tables. However, you can still delete all the rows using `DELETE FROM t;`
- Indexes, views or other advanced structures
- Aggregations
- Sorting
- Joins
- Choosing the PK, the engine chooses the first row automatically
- Datatypes like Date, Time, Boolean, Text and BLOB

General:
- ACID transactions
  - No logging or recovery management
  - No concurrency control policy
  - No durability guarantees (e.g. flushing to disk to bypass OS cache)
- Cache/buffer memory management, pages are loaded from and saved to disk on demand (high disk I/O)
- Rows that are bigger than the database page size
- In-memory mode
