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

	// for i := range 3000 {
	// 	e.Put(
	// 		fmt.Sprintf("key-%d", i),
	// 		fmt.Appendf(nil, "value-%d", i),
	// 	)
	// }

	for i := range 3000 {
		if val, ok := e.Get(fmt.Sprintf("key-%d", i)); ok {
			fmt.Println(string(val))
		} else {
			fmt.Println("missed it")
		}
	}
}
