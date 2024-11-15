package main

import (
	"encoding/binary"
	"fmt"
	"math"
)

func main() {
	fmt.Println("Hello, world")

	var a int16 = -789
	byteArray := make([]byte, 2)
	binary.BigEndian.PutUint16(byteArray, uint16(a))
	var b int16 = 789
	byteArray2 := make([]byte, 2)
	binary.BigEndian.PutUint16(byteArray2, uint16(b))
	fmt.Println(intCompare(byteArray, byteArray2, sqlSmallint))

	var aa float64 = -789.22
	byteArray = make([]byte, 8)
	binary.BigEndian.PutUint64(byteArray, math.Float64bits(aa))
	var bb float64 = 789.22
	byteArray2 = make([]byte, 8)
	binary.BigEndian.PutUint64(byteArray2, math.Float64bits(bb))
	fmt.Println(floatCompare(byteArray, byteArray2, sqlDouble))
}
