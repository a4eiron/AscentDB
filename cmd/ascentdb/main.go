package main

import (
	"fmt"
	"log"
	"time"

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
			Mode:     config.SyncNone,
			Interval: 600 * time.Millisecond,
		},
	})

	if err != nil {
		log.Fatalln(err)
	}
	defer e.Close()

	// var wg sync.WaitGroup
	// wg.Go(func() {
	// putSomeStuff(e, 0, 50000)
	// })
	// wg.Go(func() {
	// putSomeStuff(e, 0, 100000)
	// })

	// wg.Wait()
	getSomeStuff(e, 0, 200000)
}

func putSomeStuff(e *engine.Engine, start, end int) {
	counter := 0
	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%020d", i)
		value := fmt.Sprintf("value-%020d-updated", i)
		e.Put([]byte(key), []byte(value))
		counter++
		log.Println("written:", counter)
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
			// log.Println("MISSED IT", key)
		}
	}

	// log.Println("==============================================================")
	log.Println("counter:", counter)
	// log.Println("==============================================================")
}
