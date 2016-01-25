package siesta

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/snappy"
)

var snappyMagicBytes = []byte{130, 83, 78, 65, 80, 80, 89, 0}

func snappyDecode(src []byte) ([]byte, error) {
	if bytes.Equal(src[:8], snappyMagicBytes) {
		cap := uint32(len(src))
		current := uint32(16)
		result := make([]byte, 0, len(src))

		for current < cap {
			size := binary.BigEndian.Uint32(src[current : current+4])
			current += 4

			chunk, err := snappy.Decode(nil, src[current:current+size])
			if err != nil {
				return nil, err
			}
			current += size
			result = append(result, chunk...)
		}
		return result, nil
	}

	return snappy.Decode(nil, src)
}
