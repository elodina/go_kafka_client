package siesta

import "time"

type RecordAccumulatorConfig struct {
	batchSize         int
	totalMemorySize   int
	compressionType   string
	linger            time.Duration
	retryBackoff      time.Duration
	blockOnBufferFull bool
	metrics           map[string]Metric
	time              time.Time
	metricTags        map[string]string
	networkClient     *NetworkClient
}

type RecordAccumulator struct {
	config        *RecordAccumulatorConfig
	networkClient *NetworkClient
	batchSize     int
	batches       map[string]map[int32][]*ProducerRecord

	addChan      chan *ProducerRecord
	flushed      map[string]map[int32]chan bool
	closing      chan bool
	closed       chan bool
	metadataChan chan *RecordMetadata
	records      map[string]map[int32]chan *ProducerRecord
}

func NewRecordAccumulator(config *RecordAccumulatorConfig, metadataChan chan *RecordMetadata) *RecordAccumulator {
	accumulator := &RecordAccumulator{}
	accumulator.config = config
	accumulator.batchSize = config.batchSize
	accumulator.addChan = make(chan *ProducerRecord, 100) //TODO config
	accumulator.batches = make(map[string]map[int32][]*ProducerRecord)
	accumulator.networkClient = config.networkClient
	accumulator.closing = make(chan bool)
	accumulator.closed = make(chan bool)
	accumulator.metadataChan = metadataChan
	accumulator.records = make(map[string]map[int32]chan *ProducerRecord)

	go accumulator.sender()

	return accumulator
}

func (ra *RecordAccumulator) sender() {
	for {
		select {
		case <-ra.closing:
			ra.cleanup()
			return
		default:
			{
				select {
				case <-ra.closing:
					ra.cleanup()
					return
				case record := <-ra.addChan:
					ra.addRecord(record)
				}
			}
		}
	}
}

func (ra *RecordAccumulator) cleanup() {
	ra.flushAll()
	close(ra.addChan)
	ra.networkClient.close()
	ra.closed <- true
}

func (ra *RecordAccumulator) addRecord(record *ProducerRecord) {
	if ra.batches[record.Topic] == nil {
		ra.batches[record.Topic] = make(map[int32][]*ProducerRecord)
	}
	if ra.batches[record.Topic][record.partition] == nil {
		ra.createBatch(record.Topic, record.partition)
	}
	ra.records[record.Topic][record.partition] <- record
}

func (ra *RecordAccumulator) createBatch(topic string, partition int32) {
	ra.batches[topic][partition] = make([]*ProducerRecord, 0, ra.batchSize)
	ra.records[topic] = make(map[int32]chan *ProducerRecord)
	ra.records[topic][partition] = make(chan *ProducerRecord, ra.batchSize)
	go ra.watcher(topic, partition)
}

func (ra *RecordAccumulator) watcher(topic string, partition int32) {
	timeout := time.After(ra.config.linger)
	for {
		if len(ra.batches[topic][partition]) >= ra.batchSize {
			ra.flush(topic, partition, ra.batches[topic][partition])
			go ra.watcher(topic, partition)
			return
		}
		select {
		case record := <-ra.records[topic][partition]:
			ra.batches[record.Topic][record.partition] = append(ra.batches[record.Topic][record.partition], record)
		case <-timeout:
			ra.flush(topic, partition, ra.batches[topic][partition])
			go ra.watcher(topic, partition)
			return
		}
	}
}

func (ra *RecordAccumulator) flush(topic string, partition int32, batch []*ProducerRecord) {
	if len(batch) > 0 {
		ra.networkClient.send(topic, partition, batch)
		ra.batches[topic][partition] = make([]*ProducerRecord, 0, ra.batchSize)
	}
}

func (ra *RecordAccumulator) flushAll() {
	for topic, partitionBatches := range ra.batches {
		for partition, _ := range partitionBatches {
			ra.flush(topic, partition, ra.batches[topic][partition])
		}
	}
}

func (ra *RecordAccumulator) close() chan bool {
	ra.closing <- true
	return ra.closed
}
