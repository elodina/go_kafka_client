package go_kafka_client

import (
    cql "github.com/gocql/gocql"
    "sync"
)

type CassandraOffsetStorage struct {
    cluster *cql.ClusterConfig
    connection *cql.Session
    lock sync.Mutex
}

func NewCassandraOffsetStorage(host string, keyspace string) *CassandraOffsetStorage {
    cluster := cql.NewCluster(host)
    cluster.Keyspace = keyspace

    return &CassandraOffsetStorage{cluster: cluster,}
}

// Gets the offset for a given group, topic and partition.
// May return an error if fails to retrieve the offset.
func (this *CassandraOffsetStorage) GetOffset (group string, topic string, partition int32) (int64, error) {
    this.ensureConnection(group)
    response := &OffsetResponse{}
    data := this.connection.Query("SELECT group, topic, partition, last_offset, dateof(updated) as updated FROM kafka_offsets WHERE group = ? AND topic = ? AND partition = ? LIMIT 1", group, topic, partition).Iter()
    data.Scan(&response.Group, &response.Topic, &response.Partition, &response.LastOffset, &response.TraceData, &response.Updated)
    if err := data.Close(); err != nil {
        return InvalidOffset, err
    }

    return response.LastOffset, nil
}

// Commits the given offset for a given group, topic and partition.
// May return an error if fails to commit the offset.
func (this *CassandraOffsetStorage) CommitOffset (group string, topic string, partition int32, offset int64) error {
    this.ensureConnection(group)
    return this.connection.Query("INSERT INTO kafka_offsets(group, topic, partition, last_offset, updated) VALUES(?, ?, ?, ?, now())", group, topic, partition, offset).Exec()
}

func (this *CassandraOffsetStorage) ensureConnection(consumerGroup string) {
    var err error
    if (this.connection == nil) {
        inLock(&this.lock, func() {
            if (this.connection == nil) {
                this.connection, err = this.cluster.CreateSession()
                if err != nil {
                    panic(err)
                }
                this.connection.Query("CREATE TABLE IF NOT EXISTS kafka_offsets(group text, topic text, partition int, last_offset int, trace_data blob, updated timeuuid, PRIMARY KEY((group, topic, partition), last_offset, updated)) WITH CLUSTERING ORDER BY (last_offset DESC, updated DESC)").Exec()
            }
        })
    }
}

type OffsetResponse struct {
    Group string
    Topic string
    Partition int32
    LastOffset int64
    TraceData []byte
    Updated int64
}