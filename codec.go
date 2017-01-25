package redisque

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/golang/snappy"
)

var (
	DIRTY_DATA_DROPPED_ERR = errors.New("Some dirty data dropped. Longer than max uint32.")
	UNMARSHAL_HEADER_ERR   = errors.New("Unmarshal fail: unexpected header.")
	UNMARSHAL_BODY_ERR     = errors.New("Unmarshal fail: unexpected body.")
	COMPRESS_FLAG_ERR      = errors.New("Unmarshal fail: unexpected compress flag.")
	DATA_NIL_ERR           = errors.New("Unmarshal fail: data is nil.")
)

const (
	LENGHT_TYPE_UINT16 = iota
	LENGHT_TYPE_UINT32
)

const (
	FLAG_NOT_COMPRESSED = iota
	FLAG_COMPRESSED
)

const (
	COMPRESS_THRESHOLD_SIZE = 1024 * 20 // 20KB
)

type TransData struct {
	pkts [][]byte
	ts   int64
}

func marshal(data *TransData) (result []byte, err error) {
	var headLength, bodyLength int64
	headLength = 8
	for _, pkt := range data.pkts {
		bodyLength = bodyLength + int64(len(pkt)) + 2
	}
	origin := make([]byte, headLength, headLength+bodyLength)
	binary.LittleEndian.PutUint64(origin[:8], uint64(data.ts))
	for _, v := range data.pkts {
		length := len(v)
		switch {
		case length <= math.MaxUint16:
			origin = append(origin, []uint8{LENGHT_TYPE_UINT16, 0, 0}...)
			binary.LittleEndian.PutUint16(origin[len(origin)-2:], uint16(length))

		case length > math.MaxUint16 && length <= math.MaxUint32:
			origin = append(origin, []uint8{LENGHT_TYPE_UINT32, 0, 0, 0, 0}...)
			binary.LittleEndian.PutUint32(origin[len(origin)-4:], uint32(length))

		case length > math.MaxUint32:
			fallthrough
		case snappy.MaxEncodedLen(length) < 0:
			// max 3681400511: from snappy
			err = DIRTY_DATA_DROPPED_ERR
			return
		}
		origin = append(origin, v...)
	}

	if len(origin) > COMPRESS_THRESHOLD_SIZE {
		compressed := snappy.Encode(nil, origin)
		result = append(result, FLAG_COMPRESSED)
		result = append(result, compressed...)
	} else {
		result = append(result, FLAG_NOT_COMPRESSED)
		result = append(result, origin...)
	}

	return
}

func unmarshal(data []byte) (result *TransData, err error) {
	if len(data) == 0 {
		return nil, DATA_NIL_ERR
	}

	result = new(TransData)
	var origin []byte

	switch data[0] {
	case FLAG_COMPRESSED:
		compressed := data[1:]
		origin, err = snappy.Decode(nil, compressed)
		if err != nil {
			return
		}
	case FLAG_NOT_COMPRESSED:
		origin = data[1:]
	default:
		return nil, COMPRESS_FLAG_ERR
	}

	// header
	if len(origin) >= 8 {
		result.ts = int64(binary.LittleEndian.Uint64(origin[:8]))
	} else {
		return nil, UNMARSHAL_HEADER_ERR
	}

	// body
	origin = origin[8:]
	for {
		if len(origin) < 1 {
			break
		}
		typ := origin[0]
		origin = origin[1:]
		switch typ {
		case LENGHT_TYPE_UINT16:
			if len(origin) < 2 {
				return nil, UNMARSHAL_BODY_ERR
			}
			length := binary.LittleEndian.Uint16(origin[:2])
			origin = origin[2:]
			if len(origin) < int(length) || length == 0 {
				return nil, UNMARSHAL_BODY_ERR
			}
			buf := make([]byte, length)
			copy(buf, origin[:length])
			result.pkts = append(result.pkts, buf)
			origin = origin[length:]
		case LENGHT_TYPE_UINT32:
			if len(origin) < 4 {
				return nil, UNMARSHAL_BODY_ERR
			}
			length := binary.LittleEndian.Uint32(origin[:4])
			origin = origin[4:]
			if uint32(len(origin)) < length || length == 0 {
				return nil, UNMARSHAL_BODY_ERR
			}
			buf := make([]byte, length)
			copy(buf, origin[:length])
			result.pkts = append(result.pkts, buf)
			origin = origin[length:]
		default:
			return nil, UNMARSHAL_BODY_ERR
		}
	}
	return
}
