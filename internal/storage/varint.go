package storage

import (
	"encoding/binary"
	"fmt"
	"io"
)

func ReadVarint32(reader io.ByteReader) (uint32, int, error) {
	result := uint32(0)
	bytesRead := 0

	for i := 0; i < 4; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, bytesRead, err
		}

		result = result << 7
		result = result | uint32((b & 0x7f))
		bytesRead++
		if bytesRead > 1 {
			fmt.Println("%d", result)
		}
		if b&0x80 == 0 {
			break
		}
	}

	return result, bytesRead, nil
}

func ReadVarint(reader io.ByteReader) (uint64, int, error) {
	a0, err := reader.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	// If A0 is between 0 and 240 inclusive, then the result is the value of A0.
	if a0 <= 240 {
		return uint64(a0), 1, nil
	}

	// If A0 is between 241 and 248 inclusive, then the result is 240+256*(A0-241)+A1.
	if a0 >= 241 && a0 <= 248 {
		a1, err := reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		return 240 + 256*uint64(a0-241) + uint64(a1), 2, nil
	}

	// If A0 is 249 then the result is 2288+256*A1+A2.
	if a0 == 249 {
		a1, err := reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		a2, err := reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		return 2288 + 256*uint64(a1) + uint64(a2), 3, nil
	}

	var numBytes int
	// If A0 is 250 then the result is A1..A3 as a 3-byte big-ending integer.
	if a0 == 250 {
		numBytes = 3
	}
	// If A0 is 251 then the result is A1..A4 as a 4-byte big-ending integer.
	if a0 == 251 {
		numBytes = 4
	}
	// If A0 is 252 then the result is A1..A5 as a 5-byte big-ending integer.
	if a0 == 252 {
		numBytes = 5
	}
	// If A0 is 253 then the result is A1..A6 as a 6-byte big-ending integer.
	if a0 == 253 {
		numBytes = 6
	}
	// If A0 is 254 then the result is A1..A7 as a 7-byte big-ending integer.
	if a0 == 254 {
		numBytes = 7
	}
	// If A0 is 255 then the result is A1..A8 as a 8-byte big-ending integer.
	if a0 == 255 {
		numBytes = 8
	}

	bs := make([]byte, numBytes)
	for i := 0; i < len(bs); i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, 0, err
		}
		bs[i] = b
	}

	return binary.BigEndian.Uint64(bs), len(bs) + 1, nil
}

func WriteVarint32(w io.ByteWriter, x uint64) (int, error) {
	i := 0
	for x >= 0x80 {
		if err := w.WriteByte(byte(x) | 0x80); err != nil {
			return i, err
		}
		x >>= 7
	}
	if err := w.WriteByte(byte(x)); err != nil {
		return i, err
	}
	return i + 1, nil
}

func WriteVarint(w io.ByteWriter, v uint64) (int, error) {
	// If V<=240 then output a single by A0 equal to V.
	if v <= 240 {
		err := w.WriteByte(byte(v))
		if err != nil {
			return 0, err
		}
		return 1, nil
	}

	// If V<=2287 then output A0 as (V-240)/256 + 241 and A1 as (V-240)%256.
	var bs []byte
	if v <= 2287 {
		bs = append(bs, byte(0xFF&(v-240)/256+241), byte(0xFF&(v-240)%256))
	}
	// If V<=67823 then output A0 as 249, A1 as (V-2288)/256, and A2 as (V-2288)%256.
	if v <= 67823 {
		bs = append(bs, 249, byte(0xFF&(v-2288)/256), byte((v-2288)%256))
	}
	// If V<=16777215 then output A0 as 250 and A1 through A3 as a big-endian 3-byte integer.
	if v <= 16777215 {
		bs = append(bs, 250, byte(0xFF&v>>16), byte(0xFF&v>>8), byte(0xFF&v))
	}
	// If V<=4294967295 then output A0 as 251 and A1..A4 as a big-ending 4-byte integer.
	if v <= 4294967295 {
		bs = append(bs, 251, byte(0xFF&v>>24), byte(0xFF&v>>16), byte(0xFF&v>>8), byte(0xFF&v))
	}
	// If V<=1099511627775 then output A0 as 252 and A1..A5 as a big-ending 5-byte integer.
	if v <= 1099511627775 {
		bs = append(bs, 252, byte(0xFF&v>>32), byte(0xFF&v>>24), byte(0xFF&v>>16), byte(0xFF&v>>8), byte(0xFF&v))
	}
	// If V<=281474976710655 then output A0 as 253 and A1..A6 as a big-ending 6-byte integer.
	if v <= 281474976710655 {
		bs = append(bs, 253, byte(0xFF&v>>40), byte(0xFF&v>>32), byte(0xFF&v>>24), byte(0xFF&v>>16), byte(0xFF&v>>8), byte(0xFF&v))
	}
	// If V<=72057594037927935 then output A0 as 254 and A1..A7 as a big-ending 7-byte integer.
	if v <= 72057594037927935 {
		bs = append(bs, 254, byte(0xFF&v>>48), byte(0xFF&v>>48), byte(0xFF&v>>32), byte(0xFF&v>>24), byte(0xFF&v>>16), byte(0xFF&v>>8), byte(0xFF&v))
	}
	// Otherwise then output A0 as 255 and A1..A8 as a big-ending 8-byte integer.
	if v > 72057594037927935 {
		bs = append(bs, 255, byte(0xFF&v>>56), byte(0xFF&v>>48), byte(0xFF&v>>40), byte(0xFF&v>>32), byte(0xFF&v>>24), byte(0xFF&v>>16), byte(0xFF&v>>8), byte(0xFF&v))
	}

	for _, b := range bs {
		err := w.WriteByte(b)
		if err != nil {
			return 0, err
		}
	}

	return len(bs), nil
}
