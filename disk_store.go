package caskdb

import (
	"errors"
	"io/fs"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	f      *os.File
	curPos int64
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

var keyDir map[string]keyEntry = map[string]keyEntry{}

func NewDiskStore(fileName string) (*DiskStore, error) {
	var (
		fi     os.FileInfo
		err    error
		f      *os.File
		curPos int64
	)

	fi, err = os.Stat(fileName)
	if err != nil && errors.Is(err, fs.ErrExist) {
		return nil, err
	}

	if fi != nil {
		curPos = fi.Size()
	}

	if f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); err != nil {
		return nil, err
	}

	os.Stat(fileName)

	return &DiskStore{
		f:      f,
		curPos: curPos,
	}, nil
}

func (d *DiskStore) Get(key string) string {
	keyInfo, found := keyDir[key]
	if !found {
		return ""
	}

	buf := make([]byte, keyInfo.valueSize)
	_, err := d.f.ReadAt(buf, keyInfo.valuePos)
	if err != nil {
		panic(err)
	}

	_, _, val := decodeKV(buf)
	return val
}

func (d *DiskStore) Set(key string, value string) {
	timestamp := time.Now().Unix()

	dataLen, data := encodeKV(
		uint32(timestamp),
		key,
		value,
	)

	written, err := d.f.Write(data)
	if err != nil {
		panic(err)
	}
	if written != dataLen {
		panic("written != datalen")
	}

	keyDir[key] = keyEntry{
		timestamp: uint32(timestamp),
		valueSize: uint(dataLen),
		valuePos:  d.curPos,
	}
	d.curPos += int64(dataLen)

}

func (d *DiskStore) Close() bool {
	err := d.f.Close()
	if err != nil {
		panic(err)
	}

	return true
}
