package spointer

import "encoding/binary"

type SPointer struct {
	offset uint16 // 2 bytes
	length uint16 // 2 bytes
}

func NewSPointer(slotBody []byte, index uint16) *SPointer {
	return &SPointer{
		offset: binary.LittleEndian.Uint16(slotBody[index*4 : index*4+2]),
		length: binary.LittleEndian.Uint16(slotBody[index*4+2 : index*4+4]),
	}
}

// Get関係 ======================================================================

func (p *SPointer) GetOffset() uint16 {
	return p.offset
}

func (p *SPointer) GetLength() uint16 {
	return p.length
}
