package main

import (
	"fmt"
	"runtime"
)

func main() {
	fmt.Println("Hello, World! From Go 1.25")
	fmt.Println("NumCPU:", runtime.NumCPU())
	fmt.Println("GOMAXPROCS:", runtime.GOMAXPROCS(0))
}
