package caskdb

import (
	"unicode/utf8"
)

// format file provides encode/decode functions for serialisation and deserialisation
// operations
//
// format methods are generic and does not have any disk or memory specific code.
//
// The disk storage deals with bytes; you cannot just store a string or object without
// converting it to bytes. The programming languages provide abstractions where you
// don't have to think about all this when storing things in memory (i.e. RAM).
// Consider the following example where you are storing stuff in a hash table:
//
//    books = {}
//    books["hamlet"] = "shakespeare"
//    books["anna karenina"] = "tolstoy"
//
// In the above, the language deals with all the complexities:
//
//    - allocating space on the RAM so that it can store data of `books`
//    - whenever you add data to `books`, convert that to bytes and keep it in the memory
//    - whenever the size of `books` increases, move that to somewhere in the RAM so that
//      we can add new items
//
// Unfortunately, when it comes to disks, we have to do all this by ourselves, write
// code which can allocate space, convert objects to/from bytes and many other operations.
//
// This file has two functions which help us with serialisation of data.
//
//    encodeKV - takes the key value pair and encodes them into bytes
//    decodeKV - takes a bunch of bytes and decodes them into key value pairs
//
//**workshop note**
//
//For the workshop, the functions will have the following signature:
//
//    func encodeKV(timestamp uint32, key string, value string) (int, []byte)
//    func decodeKV(data []byte) (uint32, string, string)

// headerSize specifies the total header size. Our key value pair, when stored on disk
// looks like this:
//
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ timestamp │ key_size │ value_size │ key │ value │
//	└───────────┴──────────┴────────────┴─────┴───────┘
//
// This is analogous to a typical database's row (or a record). The total length of
// the row is variable, depending on the contents of the key and value.
//
// The first three fields form the header:
//
//	┌───────────────┬──────────────┬────────────────┐
//	│ timestamp(4B) │ key_size(4B) │ value_size(4B) │
//	└───────────────┴──────────────┴────────────────┘
//
// These three fields store unsigned integers of size 4 bytes, giving our header a
// fixed length of 12 bytes. Timestamp field stores the time the record we
// inserted in unix epoch seconds. Key size and value size fields store the length of
// bytes occupied by the key and value. The maximum integer
// stored by 4 bytes is 4,294,967,295 (2 ** 32 - 1), roughly ~4.2GB. So, the size of
// each key or value cannot exceed this. Theoretically, a single row can be as large
// as ~8.4GB.
const headerSize = 12

// keyEntry keeps the metadata about the KV, specially the position of
// the byte offset in the file. Whenever we insert/update a key, we create a new
// keyEntry object and insert that into keyDir.
type keyEntry struct {
	fileId    uint // todo: use later when we do multi file stuff
	valueSize uint
	valuePos  int64
	timestamp uint32
}

func NewKeyEntry(timestamp uint32, position uint32, totalSize uint32) keyEntry {
	panic("implement me")
}

func encodeHeader(timestamp uint32, keySize uint32, valueSize uint32) []byte {
	buf := make([]byte, 12)
	header{
		timestamp: timestamp,
		keySize:   keySize,
		valueSize: valueSize,
	}.WriteBytes(buf)
	return buf
}

func decodeHeader(header []byte) (uint32, uint32, uint32) {
	h, err := headerFromBytes(header)
	if err != nil {
		panic(err)
	}
	return h.timestamp, h.keySize, h.valueSize
}

func encodeKV(timestamp uint32, key string, value string) (int, []byte) {
	h := header{
		timestamp: timestamp,
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
	}

	buf := make([]byte, h.KeyLen())

	// Write Header
	h.WriteBytes(buf)

	// Write Key
	for i, val := range key {
		utf8.EncodeRune(buf[12+i:], val)
	}

	// Write value
	for i, val := range value {
		utf8.EncodeRune(buf[12+int(h.keySize)+i:], val)
	}

	return len(buf), buf

}

func decodeKV(data []byte) (uint32, string, string) {
	header, err := headerFromBytes(data[:12])
	if err != nil {
		panic(err)
	}

	return header.timestamp, string(data[12 : 12+header.keySize]), string(data[12+header.keySize:])
}
