package main

import (
	"encoding/binary"
	"fmt"
)

func main() {
	b := []byte{0, 0, 0, 0, 0, 0, 0, 1}

	// ビッグエンディアン
	i := binary.BigEndian.Uint64(b)
	fmt.Println(i) // Output: 1

	// リトルエンディアン
	i = binary.LittleEndian.Uint64(b)
	fmt.Println(i) // Output: 72057594037927936
}
