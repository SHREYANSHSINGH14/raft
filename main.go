package main

import (
	"fmt"
	"runtime/debug"

	"github.com/SHREYANSHSINGH14/raft/cmd"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
			debug.PrintStack()
		}
	}()
	cmd.Execute()
}
