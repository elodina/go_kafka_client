package go_kafka_client

import (
	cql "github.com/gocql/gocql"
	"sync"
)

type CassandraOffsetStorage struct {
	cluster    *cql.ClusterConfig
	connection *cql.Session
	lock       sync.Mutex
}

func NewCassandraOffsetStorage(host string, keyspace string) *CassandraOffsetStorage {
	cluster := cql.NewCluster(host)
	cluster.Keyspace = keyspace

	return &CassandraOffsetStorage{cluster: cluster}
}

// Gets the offset for a given group, topic and partition.
// May return an error if fails to retrieve the offset.
func (this *CassandraOffsetStorage) GetOffset(group string, topic string, partition int32) (int64, error) {
	this.ensureConnection()
	response := &OffsetResponse{}
	data := this.connection.Query(`SELECT group, topic, partition, last_offset, dateof(updated), trace_data FROM kafka_offsets WHERE group = ? AND topic = ? AND partition = ? LIMIT 1`,
		group, topic, partition)
	if err := data.Scan(&response.Group, &response.Topic, &response.Partition, &response.LastOffset,
		&response.Updated, &response.TraceData); err != nil {
		if err == cql.ErrNotFound {
			return InvalidOffset, nil
		}

		Error(this, err)
		return InvalidOffset, err
	}

	return response.LastOffset, nil
}

// Commits the given offset for a given group, topic and partition.
// May return an error if fails to commit the offset.
func (this *CassandraOffsetStorage) CommitOffset(group string, topic string, partition int32, offset int64) error {
	this.ensureConnection()
	return this.connection.Query(`INSERT INTO kafka_offsets(group, topic, partition, last_offset, updated) VALUES(?, ?, ?, ?, ?)`,
		group, topic, partition, offset, cql.TimeUUID()).Exec()
}

func (this *CassandraOffsetStorage) ensureConnection() {
	var err error
	if this.connection == nil {
		inLock(&this.lock, func() {
			if this.connection == nil {
				this.connection, err = this.cluster.CreateSession()
				if err != nil {
					panic(err)
				}
				this.connection.Query(`CREATE TABLE IF NOT EXISTS kafka_offsets(group text, topic text, partition int, last_offset int, trace_data blob, updated timeuuid, PRIMARY KEY((group, topic, partition), last_offset, updated)) WITH CLUSTERING ORDER BY (last_offset DESC, updated DESC)`).Exec()
			}
		})
	}
}

type OffsetResponse struct {
	Group      string
	Topic      string
	Partition  int64
	LastOffset int64
	TraceData  []byte
	Updated    int64
}
