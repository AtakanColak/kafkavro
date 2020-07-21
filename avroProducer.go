package kafka

import (
	"encoding/binary"
	"time"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
)

type AvroProducer struct {
	producer             sarama.SyncProducer
	SchemaRegistryClient *CachedSchemaRegistryClient
}

// NewAvroProducer is a basic producer to interact with schema registry, avro and kafka
func NewAvroProducer(kafkaServers []string, schemaRegistryServers []string) (*AvroProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond
	producer, err := sarama.NewSyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)
	return &AvroProducer{producer, schemaRegistryClient}, nil
}

// NewAvroProducerWithSASLPlain is a basic producer to interact with schema registry, avro and kafka, with Kafka SASL configured, PLAIN
func NewAvroProducerWithSASLPlain(username, password string, kafkaServers, schemaRegistryServers []string) (*AvroProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0

	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password

	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond
	producer, err := sarama.NewSyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)
	return &AvroProducer{producer, schemaRegistryClient}, nil
}

// NewAvroProducerWithSASL is a basic producer to interact with schema registry, avro and kafka, with Kafka SASL configured, scram256
func NewAvroProducerWithSASL(username, password string, kafkaServers, schemaRegistryServers []string) (*AvroProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0

	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
	config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)

	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond
	producer, err := sarama.NewSyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)
	return &AvroProducer{producer, schemaRegistryClient}, nil
}

//GetSchemaId get schema id from schema-registry service
func (ap *AvroProducer) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.SchemaRegistryClient.CreateSubject(topic+"-value", avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}

func (ap *AvroProducer) Add(topic string, schema string, key []byte, value []byte) error {

	avroCodec, err := goavro.NewCodec(schema)
	if err != nil {
		return wrap("new codec", err)
	}

	schemaId, err := ap.GetSchemaId(topic, avroCodec)
	if err != nil {
		return wrap("get schema id", err)
	}

	native, _, err := avroCodec.NativeFromTextual(value)
	if err != nil {
		return wrap("native from textual", err)
	}
	// Convert native Go form to binary Avro data
	binaryValue, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return wrap("binary from native", err)
	}

	binaryMsg := &AvroEncoder{
		SchemaID: schemaId,
		Content:  binaryValue,
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: binaryMsg,
	}
	_, _, err = ap.producer.SendMessage(msg)
	if err != nil {
		return wrap("send message", err)
	}
	return nil
}

func (ap *AvroProducer) AddBinary(codec *goavro.Codec, topic string, value, key []byte, schemaId int) error {

	native, _, err := codec.NativeFromTextual(value)
	if err != nil {
		return wrap("native from textual", err)
	}

	binaryValue, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return wrap("binary from native", err)
	}

	binaryMsg := &AvroEncoder{
		SchemaID: schemaId,
		Content:  binaryValue,
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: binaryMsg,
	}
	_, _, err = ap.producer.SendMessage(msg)
	if err != nil {
		return wrap("send message", err)
	}
	return nil
}

func (ap *AvroProducer) SendBinaries(msgs []*sarama.ProducerMessage) error {
	return ap.producer.SendMessages(msgs)
}

func (ac *AvroProducer) Close() {
	ac.producer.Close()
}

// AvroEncoder encodes schemaId and Avro message.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// Notice: the Confluent schema registry has special requirements for the Avro serialization rules,
// not only need to serialize the specific content, but also attach the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte
	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by Schema Registry
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaId and Content.
func (a *AvroEncoder) Length() int {
	return 5 + len(a.Content)
}
