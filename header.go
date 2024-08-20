package caskdb

import (
	"encoding/binary"
	"fmt"
)

type header struct {
	timestamp uint32
	keySize   uint32
	valueSize uint32
}

func (h header) WriteBytes(buf []byte) []byte {
	// assumes len(buf) >= 12
	binary.LittleEndian.PutUint32(buf, h.timestamp)
	binary.LittleEndian.PutUint32(buf[4:], h.keySize)
	binary.LittleEndian.PutUint32(buf[8:], h.valueSize)
	return buf
}

func (h header) KeyLen() int {
	return 12 + int(h.keySize) + int(h.valueSize)
}

func headerFromBytes(buf []byte) (h header, err error) {
	if len(buf) != 12 {
		return h, fmt.Errorf("invalid header size: %d bytes. Expected 12", len(buf))
	}

	return header{
		timestamp: binary.LittleEndian.Uint32(buf[:4]),
		keySize:   binary.LittleEndian.Uint32(buf[4:8]),
		valueSize: binary.LittleEndian.Uint32(buf[8:12]),
	}, nil
}
