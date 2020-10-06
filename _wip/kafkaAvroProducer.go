package kafkavro

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
)

// KafkaAvroProducer provides a kafka producer with avro encoding
type KafkaAvroProducer struct {
	producer             sarama.SyncProducer
	schemaRegistryClient srclient.ISchemaRegistryClient

	queueLock sync.Mutex
	queue     []*sarama.ProducerMessage
}

// NewKafkaAvroProducer initalizes a KafkaAvroProducer
// kafkaBrokers: 			passed directly to sarama.NewSyncProducer
// producerConfig: 			passed directly to sarama.NewSyncProducer
// schemaRegistryClient: 	used to fetch & cache avro schemas and codecs
// WARNING use ProducerConfigDefault for producerConfig, customization is at your own risk
func NewKafkaAvroProducer(kafkaBrokers []string, producerConfig *sarama.Config, schemaRegistryClient srclient.ISchemaRegistryClient) (*KafkaAvroProducer, error) {
	producer, err := sarama.NewSyncProducer(kafkaBrokers, producerConfig)
	if err != nil {
		return nil, err
	}
	return &KafkaAvroProducer{producer, schemaRegistryClient, []*sarama.ProducerMessage{}}, nil
}

// PrepareMessage to be sent as a sarama.ProducerMessage
func (kap *KafkaAvroProducer) PrepareMessage(topic string, keySchema *srclient.Schema, valueSchema *srclient.Schema, key []byte, value []byte, keyIsAvro bool, valueIsAvro bool) (*sarama.ProducerMessage, error) {
	keyEncoder, err := toSaramaEncoder(keySchema, keyIsAvro, key)
	if err != nil {
		return nil, err
	}

	valueEncoder, err := toSaramaEncoder(valueSchema, valueIsAvro, value)
	if err != nil {
		return nil, err
	}

	return &sarama.ProducerMessage{topic, keyEncoder, valueEncoder}
}

// SendMessage sends the given message
func (kap *KafkaAvroProducer) SendMessage(msg *sarama.ProducerMessage) error {
	return kap.SendMessages([]*sarama.ProducerMessage{msg})
}

// SendMessages sends the given messages
func (kap *KafkaAvroProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return kap.producer.SendMessages(msgs)
}

// PrepareMessageToQueue calls PrepareMessage and uses internalized queue to cache messages concurrently
func (kap *KafkaAvroProducer) PrepareMessageToQueue(topic string, keySchema *srclient.Schema, valueSchema *srclient.Schema, key []byte, value []byte, keyIsAvro bool, valueIsAvro bool) error {
	msg, err := kap.PrepareMessage(topic, keySchema, valueSchema, key, value, keyIsAvro, valueIsAvro)
	if err != nil {
		return err
	}
	kap.queueLock.Lock()
	kap.queue = append(kap.queue, msg)
	kap.queueLock.Unlock()
	return nil
}

// SendQueue sends the queued messages and flushes the queue
func (kap *KafkaAvroProducer) SendQueue() error {
	kap.queueLock.Lock()
	err := kap.SendMessages(kap.queue)
	kap.queue = []*sarama.ProducerMessage{}
	kap.queueLock.Unlock()
	return err
}

func toSaramaEncoder(schema *srclient.Schema, isAvro bool, value []byte) (sarama.Encoder, error) {

	if !isAvro {
		return sarama.StringEncoder(value), nil
	}

	native, _, err := schema.Codec().NativeFromTextual(value)
	if err != nil {
		return nil, err
	}

	binary, err := schema.Codec().BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	return &AvroEncoder{schema.ID(), binary}
}
