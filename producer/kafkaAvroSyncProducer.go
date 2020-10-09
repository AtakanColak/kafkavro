package producer

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
)

// KafkaAvroSyncProducer provides a kafka producer with avro encoding
// Uses "github.com/Shopify/sarama".SyncProducer
// Uses "github.com/riferrei/srclient".SchemaRegistryClient
type KafkaAvroSyncProducer struct {
	producer             sarama.SyncProducer
	schemaRegistryClient *srclient.SchemaRegistryClient

	queue     chan sarama.ProducerMessage
	queueLock sync.RWMutex

	keySchemaIDMap     map[string]int
	keySchemaIDMapLock sync.RWMutex

	valueSchemaIDMap     map[string]int
	valueSchemaIDMapLock sync.RWMutex
}

// NewKafkaAvroSyncProducer initalizes a KafkaAvroSyncProducer
// kafkaBrokers: 			passed directly to sarama.NewSyncProducer
// queueSize: 				size of the cached queues, will attempt to send when queue is full
// producerConfig: 			passed directly to sarama.NewSyncProducer
// schemaRegistryClient: 	used to fetch & cache avro schemas and codecs
// WARNING use ProducerConfigDefault for producerConfig, customization is at your own risk
func NewKafkaAvroSyncProducer(kafkaBrokers []string, queueSize int, producerConfig *sarama.Config, schemaRegistryClient *srclient.SchemaRegistryClient) (*KafkaAvroSyncProducer, error) {
	producer, err := sarama.NewSyncProducer(kafkaBrokers, producerConfig)
	if err != nil {
		return nil, err
	}
	return &KafkaAvroSyncProducer{
		producer:             producer,
		schemaRegistryClient: schemaRegistryClient,
		queue:                make(chan sarama.ProducerMessage, queueSize),
		keySchemaIDMap:       make(map[string]int),
		valueSchemaIDMap:     make(map[string]int),
	}, nil
}

// SendMessage sends the given message, doesn't send the messages in the queue
func (kap *KafkaAvroSyncProducer) SendMessage(msg *sarama.ProducerMessage) error {
	return kap.SendMessages([]*sarama.ProducerMessage{msg})
}

// SendMessages sends the given messages, doesn't send the messages in the queue
func (kap *KafkaAvroSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return kap.producer.SendMessages(msgs)
}

// GetSchemas of a given topic, schemaIDs are cached as part of optimization
// You may call FlushSchemaCache if there is an update to schemas in the schema registry
func (kap *KafkaAvroSyncProducer) GetSchemas(topic string, keyIsAvro bool, valueIsAvro bool) (keySchema *srclient.Schema, valueSchema *srclient.Schema, err error) {

	getAndCacheSchemaID := func(topic string, isAvro, isKey bool, cache map[string]int, mutex *sync.RWMutex, client *srclient.SchemaRegistryClient) (*srclient.Schema, error) {
		if !isAvro {
			return nil, nil
		}

		mutex.RLock()
		id, ok := cache[topic]
		mutex.RUnlock()
		if ok {
			return client.GetSchema(id)
		}

		schema, err := client.GetLatestSchema(topic, isKey)
		if err == nil {
			mutex.Lock()
			defer mutex.Unlock()
			cache[topic] = schema.ID()
		}
		return schema, err
	}

	keySchema, err = getAndCacheSchemaID(topic, keyIsAvro, true, kap.keySchemaIDMap, &kap.keySchemaIDMapLock, kap.schemaRegistryClient)
	if err != nil {
		return nil, nil, err
	}

	valueSchema, err = getAndCacheSchemaID(topic, valueIsAvro, false, kap.valueSchemaIDMap, &kap.valueSchemaIDMapLock, kap.schemaRegistryClient)
	if err != nil {
		return nil, nil, err
	}

	return keySchema, valueSchema, nil
}

// FlushSchemaIDCache to get latest schemas
func (kap *KafkaAvroSyncProducer) FlushSchemaIDCache() {
	kap.keySchemaIDMapLock.Lock()
	kap.keySchemaIDMap = make(map[string]int)
	kap.keySchemaIDMapLock.Unlock()

	kap.valueSchemaIDMapLock.Lock()
	kap.valueSchemaIDMap = make(map[string]int)
	kap.valueSchemaIDMapLock.Unlock()
}

// PrepareProducerMessageToQueue calls GetSchemas and PrepareProducerMessage and uses internalized queue to cache messages concurrently
// key and value must be AVRO PARSED ALREADY
func (kap *KafkaAvroSyncProducer) PrepareProducerMessageToQueue(topic string, key []byte, value []byte, keyIsAvro bool, valueIsAvro bool) error {
	keySchema, valueSchema, err := kap.GetSchemas(topic, keyIsAvro, valueIsAvro)
	if err != nil {
		return err
	}

	msg, err := PrepareProducerMessage(topic, keySchema, valueSchema, key, value, keyIsAvro, valueIsAvro)
	if err != nil {
		return err
	}

	return kap.AddMessageToQueue(msg)
}

// AddMessageToQueue is safe to call concurrently. If queue is full, calls SendQueue beforehand.
func (kap *KafkaAvroSyncProducer) AddMessageToQueue(msg sarama.ProducerMessage) error {
	// if queue is full, send messages
	if len(kap.queue) == cap(kap.queue) {
		if err := kap.SendQueue(); err != nil {
			return err
		}
	}
	select {
	case kap.queue <- msg:
		return nil // message is added
	default:
		return kap.AddMessageToQueue(msg) // couldn't add the message, queue must have been filled in the meantime, do recursion
	}
}

// SendQueue sends the queued messages and flushes the queue
func (kap *KafkaAvroSyncProducer) SendQueue() error {
	kap.queueLock.Lock()
	quelen := len(kap.queue)
	// queue might have been emptied while waiting for the unlock
	if quelen == 0 {
		return nil
	}
	values := make([]sarama.ProducerMessage, quelen)
	references := make([]*sarama.ProducerMessage, quelen)
	for i := 0; i < quelen; i++ {
		values[i] = <-kap.queue
		references[i] = &values[i]
	}
	kap.queueLock.Unlock() // not deferred above because sending messages might take time
	return kap.SendMessages(references)
}
