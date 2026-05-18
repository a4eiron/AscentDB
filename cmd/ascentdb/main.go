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

	log.Println("At the start:", runtime.NumGoroutine())
	log.SetFlags(log.Lshortfile)

	num := 20000

	e, err := engine.New(&config.Options{
		DataDir:         "./data",
		BlockSize:       64 * 1024,
		MemtableSize:    64 * 1024,
		CrashRecovery:   true,
		WALSyncInterval: 800 * time.Millisecond,
	})

	if err != nil {
		log.Fatalln(err)
	}
	defer e.Close()

	// for i := range num {
	// 	key := fmt.Sprintf("key-%d", i)
	// 	value := fmt.Sprintf("value-%d", i)
	// 	e.Put(key, []byte(value))
	// }
	//
	// for i := range 1000 {
	// 	key := fmt.Sprintf("key-%d", i)
	// 	e.Delete(key)
	// }

	counter := 0
	for i := range num {
		key := fmt.Sprintf("key-%d", i)
		if val, ok := e.Get(key); ok {
			counter++
			fmt.Println(string(val))
		} else {
			fmt.Println(key, "MISSED")
		}
	}

	log.Println(counter)
	log.Println("At the end:", runtime.NumGoroutine())

}
