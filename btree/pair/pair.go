package pair

import (
	"encoding/binary"

	"github.com/yuya-isaka/chibidb/btree/spointer"
)

type Pair struct {
	key   []byte
	value []byte
}

func NewPair(slotBody []byte, pointer *spointer.SPointer) *Pair {
	offset := pointer.GetOffset()
	length := pointer.GetLength()

	keyStartOffset := offset + 2
	keyEndOffset := keyStartOffset + binary.LittleEndian.Uint16(slotBody[offset:offset+2])

	return &Pair{
		key:   slotBody[keyStartOffset:keyEndOffset],
		value: slotBody[keyEndOffset : offset+length],
	}
}

// Get関係 ======================================================================

func (p *Pair) GetKey() []byte {
	return p.key
}

func (p *Pair) GetValue() []byte {
	return p.value
}

func (p *Pair) GetSize() uint16 {
	return uint16(len(p.key) + len(p.value))
}

func (p *Pair) GetKeySize() uint16 {
	return uint16(len(p.key))
}
