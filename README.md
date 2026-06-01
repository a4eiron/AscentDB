# AscentDB

AscentDB is a persistent key-value storage engine written in Go, implementing an LSM-tree architecture from scratch.

### Usage
```go
import (
    "github.com/a4eiron/ascentdb/internal/config"
    "github.com/a4eiron/ascentdb/internal/engine"
)

e, err := engine.New(&config.Options{
    DataDir: "./data",
    BlockSize: 16 * 1024,               // 16 KB blocks
    MemtableSize: 4 * 1024 * 1024       // 4 MB memtable
    CrashRecovery: true,
    SyncOptions = config.SyncOptions {
        Mode: config.SyncBatch          // group commit
    }
})

if err != nil{
    log.Fatal(err)
}
defer e.Close()
```

### Put & Get

```go
err := e.Put([]byte("user:1:name"), []byte("a4eiron"))
// if err != nil {
// ...
// }

val, ok := e.Get([]byte("user:1:name")); 
if ok {
    fmt.Println(string(val))
}
```

### Delete

```go
e.Delete([]byte("user:1:name"))
```


### Batch writes
```go
import "github.com/a4eiron/ascentdb/internal/batch"

b := batch.New()
b.Put([]byte("key1"), []byte("val1"))
    .Put([]byte("key2"), []byte("val2"))
    .Delete([]byte("old-key"))

err := e.WriteBatch(b)
// ...
```

### Snapshots
Snapshots give you a consistent point-in-time view. Reads through a snapshot are not affected by concurrent writes.
```go
err := e.Put([]byte("key-1"), []byte("val-1"))
// ...

snap := e.NewSnapshot()             // get a snapshot before delete
defer snap.Release()                // always release to avoid leaks

e.Delete([]byte("key-1"))           // delete the key

val, ok := snap.Get([]byte("key-1")) 
if ok {
    fmt.Println(string(val))        // 'val-1'
}

val, ok = e.Get([]byte("key-1"))
if !ok {
    fmt.Println("key-1 is deleted") // should print this
}
```

### What are missing?
- [x] Write-Ahead-Log with crash recovery
- [x] SSTable with block-based layout
- [x] K-way merge compaction with min-heap
- [x] Range scans
- [x] Snapshots
- [x] Batch writes
- [x] Tombstone-aware compaction
- [ ] Custom comparator
- [ ] Compresssion
- [ ] Prefix scan
- [ ] Ratelimiting for compaction
