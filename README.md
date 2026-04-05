# Concurrent In-Memory Database

A high-performance, thread-safe in-memory database built from scratch using Java and Spring Boot, supporting transactions, isolation levels, indexing, and concurrent query execution.

---

## Features

### Core Database

* Key-value based table storage
* Dynamic schema (no fixed columns)
* CRUD operations (Create, Read, Update, Delete)

### Transactions (ACID-inspired)

* Begin / Commit / Rollback support
* Transaction context isolation
* Read-your-writes consistency

### Concurrency Control

* Fine-grained locking (Read/Write locks)
* Thread-safe using `ConcurrentHashMap`
* Deadlock prevention using timeout-based locking

### Isolation Levels

* **READ COMMITTED** → Reads only committed data
* **REPEATABLE READ** → Snapshot-based consistent reads
* **SERIALIZABLE** → Strict isolation using lock holding

### Conflict Handling

* Optimistic locking using versioning
* Prevents lost updates
* Detects concurrent modification conflicts

### Query Engine

* Search by column value
* AND queries (multi-column filters)
* OR queries (multi-value filters)
* Range queries (`>`, `<`, `>=`, `<=`)

### Pagination & Sorting

* Page-based results
* Custom sorting (ASC / DESC)

### Indexing

* Hash-based indexing for equality queries
* Tree-based indexing for range queries
* Reduces search from **O(n)** → **O(log n)**

### Persistence

* Data saved to disk (JSON)
* Auto-reload on restart

---

## System Design

```text
Client → Controller → Service → 
Transaction Manager → Lock Manager → 
In-Memory Database → Persistence Layer
```

---

## Key Components

* `DatabaseService` → Core business logic
* `TransactionManager` → Manages transactions
* `LockManager` → Handles read/write locks
* `Table` → Stores rows + indexes
* `Row` → Data container with versioning
* `PersistenceService` → Disk storage

---

## Testing

### Manual Testing

* REST APIs tested via Postman

### Concurrency Testing

* Multi-threaded stress test (20 threads)
* Verified:

  * No deadlocks
  * No data corruption
  * Proper timeout handling

---

## Sample APIs

### Create

```http
POST /db/users/1
{
  "name": "Keshav",
  "age": 22
}
```

### Read

```http
GET /db/users/1
```

### Transaction

```http
POST /db/tx/begin?level=REPEATABLE_READ
POST /db/users/1?txId=abc
POST /db/tx/commit?txId=abc
```

### Search

```http
GET /db/users/search?column=name&value=Keshav
GET /db/users/search-or?name=A&name=B
GET /db/users/search-range?column=age&op=>&value=20
```

---

## Performance Highlights

* Thread-safe concurrent operations
* Efficient indexing for fast queries
* Lock-based + optimistic concurrency hybrid
* Handles high contention scenarios

---

## What I Learned

* Designing concurrent systems
* Implementing transaction isolation
* Deadlock handling strategies
* Building indexing and query engines
* Real-world database internals

---

## Future Improvements

* Write-Ahead Logging (WAL)
* Distributed replication
* SQL-like query parser
* Fine-grained lock optimization

---

## Author

**Keshav Aggarwal**

---

