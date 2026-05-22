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
		DataDir:         "./data",
		BlockSize:       64 * 1024,
		MemtableSize:    64 * 1024,
		CrashRecovery:   true,
		WALSyncInterval: 600 * time.Millisecond,
	})

	if err != nil {
		log.Fatalln(err)
	}
	defer e.Close()

	// putSomeStuff(e)
	// getSomeStuff(e)

	// iter := e.Scan(
	// 	fmt.Sprintf("key-%020d", 13),
	// 	fmt.Sprintf("key-%020d", 25),
	// )

	// for ; iter.Valid(); iter.Next() {
	// 	fmt.Println(iter.Key(), iter.Valid())
	// }

	snap := e.NewSnapshot()

	e.Put(
		fmt.Sprintf("key-%020d", 4342),
		fmt.Appendf(nil, "value-%020d-updated", 4342),
	)

	val, _ := snap.Get(fmt.Sprintf("key-%020d", 4342))
	fmt.Println("snapshot val: ", string(val))
	val, _ = e.Get(fmt.Sprintf("key-%020d", 4342))
	fmt.Println("live val: ", string(val))

}

func putSomeStuff(e *engine.Engine) {
	for i := 0; i < 300000; i++ {
		key := fmt.Sprintf("key-%020d", i)
		value := fmt.Sprintf("value-%020d", i)
		e.Put(key, []byte(value))
	}
}

func getSomeStuff(e *engine.Engine) {
	counter := 0
	for i := 0; i < 300000; i++ {
		key := fmt.Sprintf("key-%020d", i)
		if val, ok := e.Get(key); ok {
			counter++
			fmt.Println(string(val))
		} else {
			fmt.Println("MISSED IT", key)
		}
	}

	log.Println("counter:", counter)
}
