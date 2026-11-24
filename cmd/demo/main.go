package main

import (
	"fmt"
	"os"

	"accord"
)

func main() {
	dir, err := os.MkdirTemp("", "accord-demo")
	if err != nil {
		fail(err)
	}
	defer os.RemoveAll(dir)

	cluster, err := accord.Start(dir, []string{"a", "b", "c"})
	if err != nil {
		fail(err)
	}
	defer cluster.Close()

	if err := cluster.Campaign("a"); err != nil {
		fail(err)
	}
	if err := cluster.Put("color", "blue"); err != nil {
		fail(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		value, err := cluster.Get(id, "color")
		if err != nil {
			fail(err)
		}
		fmt.Printf("color %s\n", value)
	}
	token, err := cluster.Lock("payments", "worker-1")
	if err != nil {
		fail(err)
	}
	fmt.Printf("payments %d worker-1\n", token)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
