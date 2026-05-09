package main

import (
	"fmt"
	"log"

	"github.com/a4eiron/ascentdb/internal/config"
	"github.com/a4eiron/ascentdb/internal/engine"
)

func main() {
	log.SetFlags(log.Lshortfile)

	e, err := engine.New(&config.Options{
		DataDir:       "./data",
		BlockSize:     64 * 1024,
		MemtableSize:  64 * 1024,
		CrashRecovery: true,
	})

	if err != nil {
		log.Fatalln(err)
	}

	for i := range 3000 {
		e.Put(
			fmt.Sprintf("key-%d", i),
			fmt.Appendf(nil, "value-%d", i),
		)
	}

	counter := 0
	for i := range 3000 {
		key := fmt.Sprintf("key-%d", i)
		if val, ok := e.Get(key); ok {
			counter++
			fmt.Println(string(val))
		} else {
			fmt.Println("missed key", key)

		}
	}
	log.Println("counter:", counter)
}
