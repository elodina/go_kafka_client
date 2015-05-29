package go_kafka_client

import (
    cql "github.com/gocql/gocql"
    "sync"
    "fmt"
    "strings"
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
    data := this.connection.Query(fmt.Sprintf("SELECT * FROM %s WHERE topic = ? AND partition = ? LIMIT 1", strings.Replace(group, "-", "", -1)), topic, partition).Iter()
    data.Scan(&response.Topic, &response.Partition, &response.LastOffset, &response.TraceData, &response.Updated)
    if err := data.Close(); err != nil {
        return InvalidOffset, err
    }

    return response.LastOffset, nil
}

// Commits the given offset for a given group, topic and partition.
// May return an error if fails to commit the offset.
func (this *CassandraOffsetStorage) CommitOffset (group string, topic string, partition int32, offset int64) error {
    this.ensureConnection(group)
    return this.connection.Query(fmt.Sprintf("UPDATE %s SET last_offset = ?, updated = dateof(now()) WHERE topic = ? AND partition = ?", strings.Replace(group, "-", "", -1)), offset, topic, partition).Exec()
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
                query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(topic text, partition int, last_offset int, trace_data blob, updated timestamp, PRIMARY KEY(topic, partition))", strings.Replace(consumerGroup, "-", "", -1))
                this.connection.Query(query).Exec()
            }
        })
    }
}

type OffsetResponse struct {
    Topic string
    Partition int32
    LastOffset int64
    TraceData []byte
    Updated int64
}