package db

import (
	"encoding/binary"
	"fmt"
)

func uint64ToBytes(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func bytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("expected 8 bytes, got %d", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}

func uintToBytes(v uint) []byte {
	return uint64ToBytes(uint64(v))
}

func bytesToUint(b []byte) (uint, error) {
	v, err := bytesToUint64(b)
	return uint(v), err
}

func logKey(index uint64) []byte {
	prefix := []byte(LogPrefix)
	k := make([]byte, len(prefix)+8) // 8 bytes for uint64
	copy(k, prefix)
	binary.BigEndian.PutUint64(k[len(prefix):], index)
	return k
}
