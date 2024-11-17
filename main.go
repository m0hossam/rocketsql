package main

import (
	"fmt"
)

func main() {
	fmt.Println("Hello, world")

	err := createDB("db.rocketsql")
	if err != nil {
		return
	}

	_, err = getFirstFreePagePtr(dbFilePath)
	if err != nil {
		return
	}
}
