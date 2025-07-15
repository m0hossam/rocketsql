## RocketSQL 🚀

This is a _**work-in-progress**_ database engine built from scratch inspired by SQLite, Database Design and Implementation by Edward Sciore, CMU's Intro to Database Systems, and various other sources.

## Supported SQL

### Queries:

```tsql
SELECT * FROM t
SELECT a, b, c FROM t
SELECT a, b, c FROM t WHERE a = 5
SELECT * FROM x, y WHERE x_id = y_id
```

### DDL:

```tsql
CREATE TABLE t (c1 INT, c2 FLOAT, c3 VARCHAR(32))
DROP TABLE t
TRUNCATE TABLE t
```

### DML:

```tsql
INSERT INTO t (c1, c2, c3) VALUES (1, 12.55, 'Hello, world')
INSERT INTO t (c3, c1, c2) VALUES ('Look at you, hacker', 2, 15.88)
INSERT INTO t VALUES (3, 99.99, 'Aloha')
```

```tsql
DELETE FROM t
DELETE FROM t WHERE a = 5
```

```tsql
UPDATE t SET a = 15
UPDATE t SET a = 15 WHERE b = 'Guava'
```

## Meta-Commands

- `.dump_table <table name>` dumps the contents of a table's B-Tree in a text file
- `.dump_page <page number>` dumps the contents of a page in a text file
- `.rebuild_table <table name>` rebuilds and vacuums a table, useful after multiple deletions, saves space by freeing the empty pages
- `.vacuum` removes all unused pages from the end of the database file, reducing its size, useful after using `.rebuild_table`

Notes:
- You can query the schema table using one of the following queries:
  - `SELECT * FROM rocketsql_schema`
  - `SELECT table_name, root_page_no, table_schema FROM rocketsql_schema` 
- All operations are done through B-Tree algorithms.
- Due to the balanced nature of the B-Tree data structure, the records are sorted in ascending order according to their primary keys, this allows for binary search retrieval.
- Insertion sometimes causes the B-Tree to split nodes, rearrange the records among the nodes (i.e. load-balancing) and increase the tree’s depth by one.
- Deletion causes the removal of a record from its containing leaf page only, there might exist a key pointing to the deleted record in one of the interior pages. This is a little space-wasteful. You should investigate the contents of the table using `.dump_table <t>` and use `.rebuild_table <t>` if you find too many empty pages.
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
- The delete operation does not remove interior B-Tree keys or decrease the depth of the B-Tree. This is space wasteful, but it avoids the complexity of load balancing after deletion. We provide the `.rebuild_table <t>` and `.vacuum` meta-commands for the user to use if they wish to save some space.
- The delete operation does not free leaf pages after the become empty. Again, the user can fix this using `.rebuild_table` and `.vacuum` as per needed.
- More testing is needed.

SQL:
- Query optimization not implemented
- Semicolon termination not implemented, cannot do multi-line statements
- `NULL` handling is not implemented

## Unsupported Features

SQL:
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
