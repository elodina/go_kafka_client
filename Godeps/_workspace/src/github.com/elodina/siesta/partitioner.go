package siesta

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"time"
)

type Partitioner interface {
	Partition(record *ProducerRecord, partitions []int32) (int32, error)
}

type HashPartitioner struct {
	random *RandomPartitioner
	hasher hash.Hash32
}

func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{
		random: NewRandomPartitioner(),
		hasher: fnv.New32a(),
	}
}

func (hp *HashPartitioner) Partition(record *ProducerRecord, partitions []int32) (int32, error) {
	if record.Key == nil {
		return hp.random.Partition(record, partitions)
	} else {
		hp.hasher.Reset()
		_, err := hp.hasher.Write(record.encodedKey)
		if err != nil {
			return -1, err
		}

		hash := int32(hp.hasher.Sum32())
		if hash < 0 {
			hash = -hash
		}

		return hash % int32(len(partitions)), nil
	}
}

type RandomPartitioner struct {
	random *rand.Rand
}

func NewRandomPartitioner() *RandomPartitioner {
	return &RandomPartitioner{
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (rp *RandomPartitioner) Partition(record *ProducerRecord, partitions []int32) (int32, error) {
	return rp.random.Int31n(int32(len(partitions))), nil
}
