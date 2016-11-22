package producer

import (
	"errors"
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

type RandomPartitioner struct{}

func NewRandomPartitioner() *RandomPartitioner {
	rand.Seed(time.Now().UnixNano())
	return new(RandomPartitioner)
}

func (rp *RandomPartitioner) Partition(record *ProducerRecord, partitions []int32) (int32, error) {
	return rand.Int31n(int32(len(partitions))), nil
}

type ManualPartitioner struct{}

func NewManualPartitioner() *ManualPartitioner {
	return new(ManualPartitioner)
}

func (mp *ManualPartitioner) Partition(record *ProducerRecord, partitions []int32) (int32, error) {
	if record.Partition >= int32(len(partitions)) {
		Logger.Warn("Invalid partition %d. Available partitions: %v", record.Partition, partitions)
		return -1, errors.New("Invalid partition")
	}

	return record.Partition, nil
}
