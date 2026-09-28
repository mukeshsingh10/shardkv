package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mukeshsingh10/shardkv"
)

func main() {
	path := "wal.log"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	store, err := shardkv.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: ", err)
		os.Exit(1)
	}
	defer store.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		fields := strings.SplitN(scanner.Text(), " ", 3)
		if fields[0] == "" || len(fields) == 0 {
			continue
		}

		switch strings.ToUpper(fields[0]) {
		case "SET":
			if len(fields) != 3 {
				fmt.Println("usage: SET key value")
				continue
			}
			if err := store.Set(fields[1], fields[2]); err != nil {
				fmt.Println("error:", err)
				continue
			}
			fmt.Println("OK")
		case "DELETE":
			if len(fields) != 2 {
				fmt.Println("usage: DELETE key")
				continue
			}
			if err := store.Delete(fields[1]); err != nil {
				fmt.Println("error", err)
				continue
			}
			fmt.Println("DELETED")
		case "GET":
			if len(fields) != 2 {
				fmt.Println("usage: GET key")
				continue
			}
			val, err := store.Get(fields[1])
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println(val)
		case "QUIT", "EXIT":
			return
		default:
			fmt.Println("unknown command:", fields[0])
		}
	}
}
