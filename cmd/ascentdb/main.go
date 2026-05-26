package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/a4eiron/ascentdb/internal/config"
	"github.com/a4eiron/ascentdb/internal/engine"
)

func main() {
	log.SetFlags(log.Lshortfile)

	e, err := engine.New(&config.Options{
		DataDir:         "./data",
		BlockSize:       16 * 1024,
		MemtableSize:    4 * 1024 * 1024,
		CrashRecovery:   true,
		WALSyncInterval: 600 * time.Millisecond,
	})

	if err != nil {
		log.Fatalln(err)
	}
	defer e.Close()

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// putSomeStuff(e, 1000000, 1200000)
	getSomeStuff(e, 0, 1200000)
	// snap := e.NewSnapshot()

	// start := fmt.Sprintf("key-%020d", 120)
	// end := fmt.Sprintf("key-%020d", 130)

	// iter := snap.Scan([]byte(start), []byte(end))
	// log.Println("snapshot=======")
	// for ; iter.Valid(); iter.Next() {
	// 	fmt.Println(string(iter.Key().UserKey), string(iter.Value()))
	// }

	// e.Delete(fmt.Appendf(nil, "key-%020d", 126))
	// e.Delete(fmt.Appendf(nil, "key-%020d", 127))
	// e.Delete(fmt.Appendf(nil, "key-%020d", 130))

	// iter := e.Scan([]byte(start), []byte(end))
	// log.Println("live=======")
	// for ; iter.Valid(); iter.Next() {
	// 	fmt.Println(string(iter.Key().UserKey), string(iter.Value()))
	// }

	runtime.ReadMemStats(&m2)

	fmt.Printf("Allocations during workload:\n")
	fmt.Printf("  HeapAlloc:   %d → %d bytes\n", m1.HeapAlloc, m2.HeapAlloc)
	fmt.Printf("  HeapObjects: %d → %d\n", m1.HeapObjects, m2.HeapObjects)
	fmt.Printf("  Mallocs:     %d\n", m2.Mallocs-m1.Mallocs)

}

func putSomeStuff(e *engine.Engine, start, end int) {
	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%020d", i)
		value := fmt.Sprintf("value-%020d-updated", i)
		e.Put([]byte(key), []byte(value))
	}
}

func getSomeStuff(e *engine.Engine, start, end int) {
	counter := 0
	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%020d", i)
		if val, ok := e.Get([]byte(key)); ok {
			counter++
			log.Println(string(val))
		} else {
			log.Println("MISSED IT", key)
		}
	}

	// log.Println("==============================================================")
	log.Println("counter:", counter)
	// log.Println("==============================================================")
}
