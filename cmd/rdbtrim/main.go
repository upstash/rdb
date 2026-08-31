// rdbtrim removes the bytes that follow the RDB content in the given file.
//
// Dragonfly writes its snapshots through an aligned direct-IO buffer and
// flushes the last block padded out to the block size. The padding is not
// zeroed, so it holds whatever occupied those buffer slots earlier. Such a
// file carries a complete snapshot, but a read that requires a strict EOF
// rejects it.
package main

import (
	"fmt"
	"os"

	"github.com/upstash/rdb"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: rdbtrim <file.rdb>")
		os.Exit(2)
	}

	path := os.Args[1]

	removed, err := rdb.TrimFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
		os.Exit(1)
	}

	fmt.Printf("%s: removed %d bytes\n", path, removed)
}
