package producer

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
)

// KafkaAvroSyncProducer provides a kafka producer with avro encoding
// Uses "github.com/Shopify/sarama".SyncProducer
// Uses "github.com/AtakanColak/srclient".SchemaRegistryClient
type KafkaAvroSyncProducer struct {
	producer             sarama.SyncProducer
	schemaRegistryClient srclient.ISchemaRegistryClient

	queue     chan sarama.ProducerMessage
	queueLock sync.RWMutex
}

// NewKafkaAvroSyncProducer initalizes a KafkaAvroSyncProducer
// producer: 			sarama.SyncProducer instance to use
// queueSize: 				size of the cached queues, will attempt to send when queue is full
// schemaRegistryClient: 	used to fetch & cache avro schemas and codecs
// WARNING you should use ProducerConfigDefault for SyncProducer, customization is at your own risk
func NewKafkaAvroSyncProducer(queueSize int, producer sarama.SyncProducer, schemaRegistryClient srclient.ISchemaRegistryClient) (*KafkaAvroSyncProducer, error) {
	return &KafkaAvroSyncProducer{
		producer:             producer,
		schemaRegistryClient: schemaRegistryClient,
		queue:                make(chan sarama.ProducerMessage, queueSize),
	}, nil
}

// SendMessage sends the given message, doesn't send the messages in the queue
// Use SendMessages instead if you have multiple messages to send
func (kap *KafkaAvroSyncProducer) SendMessage(msg *sarama.ProducerMessage) error {
	return kap.SendMessages([]*sarama.ProducerMessage{msg})
}

// SendMessages sends the given messages, doesn't send the messages in the queue
func (kap *KafkaAvroSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return kap.producer.SendMessages(msgs)
}

// GetSchemas of a given topic, schemaIDs are cached as part of optimization
func (kap *KafkaAvroSyncProducer) GetSchemas(topic string, keyIsAvro bool, valueIsAvro bool) (keySchema *srclient.Schema, valueSchema *srclient.Schema, err error) {

	getSchema := func(isAvro, isKey bool) (*srclient.Schema, error) {
		if !isAvro {
			return nil, nil
		}
		return kap.schemaRegistryClient.GetSchemaBySubject(topic, isKey)
	}

	keySchema, err = getSchema(keyIsAvro, true)
	if err != nil {
		return nil, nil, err
	}

	valueSchema, err = getSchema(valueIsAvro, false)
	if err != nil {
		return nil, nil, err
	}

	return keySchema, valueSchema, nil
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

// PrepareProducerMessage to be sent as a sarama.ProducerMessage
// key and value must be AVRO PARSED ALREADY
func PrepareProducerMessage(topic string, keySchema *srclient.Schema, valueSchema *srclient.Schema, key []byte, value []byte, keyIsAvro bool, valueIsAvro bool) (sarama.ProducerMessage, error) {
	keyEncoder, err := newAvroEncoder(keySchema, keyIsAvro, key)
	if err != nil {
		return sarama.ProducerMessage{}, err
	}

	valueEncoder, err := newAvroEncoder(valueSchema, valueIsAvro, value)
	if err != nil {
		return sarama.ProducerMessage{}, err
	}

	return sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: valueEncoder,
	}, nil
}
