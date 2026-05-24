package main

import (
	"fmt"
	"log"
	"sync"
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

	// putSomeStuff(e, 0, 100000)

	var wg sync.WaitGroup

	wg.Go(func() {
		getSomeStuff(e, 0, 50000)
	})
	wg.Go(func() {
		getSomeStuff(e, 0, 100000)
	})

	wg.Wait()

}

func putSomeStuff(e *engine.Engine, start, end int) {
	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%020d", i)
		value := fmt.Sprintf("value-%020d", i)
		e.Put(key, []byte(value))
	}
}

func getSomeStuff(e *engine.Engine, start, end int) {
	counter := 0
	for i := start; i < end; i++ {
		key := fmt.Sprintf("key-%020d", i)
		if val, ok := e.Get(key); ok {
			counter++
			log.Println(string(val))
		} else {
			log.Println("MISSED IT", key)
		}
	}

	log.Println("==============================================================")
	log.Println("counter:", counter)
	log.Println("==============================================================")
}
