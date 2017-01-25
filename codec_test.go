package redisque

import (
	"math"
	"testing"
	"time"
)

func TestCodec(t *testing.T) {
	// two small pkt
	var pktSmall []byte
	pktSmall = []byte("1234")

	origin := &TransData{
		pkts: [][]byte{pktSmall, pktSmall},
		ts:   time.Now().Unix(),
	}

	data, err := marshal(origin)
	if err != nil {
		t.Errorf("Test marshal fail: %v", err)
	}

	unmarshaled, err := unmarshal(data)
	if err != nil {
		t.Errorf("Test unmarshal fail: %v", err)
	}

	if !compareTransData(origin, unmarshaled) {
		t.Errorf("Test codec fail: unmarshal result changed.")
	}

	// two big pkt
	var pktBig []byte
	pktBig = make([]byte, 0, math.MaxUint16+10)
	for i := 0; i < math.MaxUint16+10; i++ {
		pktBig = append(pktBig, 1)
	}

	origin = &TransData{
		pkts: [][]byte{pktBig, pktBig},
		ts:   time.Now().Unix(),
	}

	data, err = marshal(origin)
	if err != nil {
		t.Errorf("Test marshal fail: %v", err)
	}

	unmarshaled, err = unmarshal(data)
	if err != nil {
		t.Errorf("Test unmarshal fail: %v", err)
	}

	if !compareTransData(origin, unmarshaled) {
		t.Errorf("Test codec fail: unmarshal result changed.")
	}
}

func compareTransData(data1, data2 *TransData) (equaled bool) {
	if data1.ts != data2.ts || len(data1.pkts) != len(data2.pkts) {
		return false
	}

	for i := 0; i < len(data1.pkts); i++ {
		v1 := data1.pkts[i]
		v2 := data2.pkts[i]
		if len(v1) != len(v2) {
			return false
		}

		for j := 0; j < len(v1); j++ {
			if v1[j] != v2[j] {
				return false
			}
		}
	}

	return true
}
