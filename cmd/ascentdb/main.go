package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/a4eiron/ascentdb/internal/config"
	"github.com/a4eiron/ascentdb/internal/engine"
)

func main() {
	log.SetFlags(log.Lshortfile)

	e, err := engine.New(&config.Options{
		DataDir:       "./data",
		BlockSize:     16 * 1024,
		MemtableSize:  4 * 1024 * 1024,
		CrashRecovery: true,
		SyncOptions: config.SyncOptions{
			Mode: config.SyncNone,
		},
	})

	if err != nil {
		log.Fatalln(err)
	}
	defer e.Close()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	putSomeStuff(e, 0, 100)
	getSomeStuff(e, 0, 100)

	iter := e.Scan(fmt.Appendf(nil, "key-%03d", 10), fmt.Appendf(nil, "key-%03d", 20))
	defer iter.Release()
	for iter.Valid() {
		fmt.Println(string(iter.Key()), string(iter.Value()))
		iter.Next()
	}

}

func putSomeStuff(e *engine.Engine, start, end int) {
	counter := 0
	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%020d-updated", i)
		e.Put([]byte(key), []byte(value))
		counter++
		// log.Println("written:", counter)
	}
}

func getSomeStuff(e *engine.Engine, start, end int) {
	counter := 0
	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if _, ok := e.Get([]byte(key)); ok {
			counter++
			// log.Println(string(val))
		} else {
			// log.Println("MISSED IT", key)
		}
	}

	log.Println("counter:", counter)
}
