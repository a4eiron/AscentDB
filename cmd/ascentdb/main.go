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

	num := 30000

	for i := range num {
		key := fmt.Sprintf("key-%06d", i)
		value := fmt.Sprintf("value-%06d-updated", i)
		e.Put(key, []byte(value))
	}

	for i := range num - 2000 - 422 {
		key := fmt.Sprintf("key-%06d", i)
		e.Delete(key)
	}

	count := 0
	for i := range num {
		key := fmt.Sprintf("key-%06d", i)
		if val, ok := e.Get(key); ok {
			count++
			fmt.Println(string(val))
		} else {
			fmt.Println(key, "not found")
		}
	}

	fmt.Println("count:", count, num-422)

}
