package producer

import (
	"encoding/binary"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
)

// avroEncoder encodes schemaID and Avro message.
type avroEncoder struct {
	SchemaID int
	Content  []byte
}

// Encode for encoding
// Notice: the Confluent schema registry has special requirements for the Avro serialization rules,
// not only need to serialize the specific content, but also attach the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func (a *avroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte
	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by Schema Registry
	binarySchemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaID, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaID...)
	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaID and Content.
func (a *avroEncoder) Length() int {
	return 5 + len(a.Content)
}

// PrepareProducerMessage to be sent as a sarama.ProducerMessage
// key and value must be AVRO PARSED ALREADY
func PrepareProducerMessage(topic string, keySchema *srclient.Schema, valueSchema *srclient.Schema, key []byte, value []byte, keyIsAvro bool, valueIsAvro bool) (sarama.ProducerMessage, error) {
	keyEncoder, err := toSaramaEncoder(keySchema, keyIsAvro, key)
	if err != nil {
		return sarama.ProducerMessage{}, err
	}

	valueEncoder, err := toSaramaEncoder(valueSchema, valueIsAvro, value)
	if err != nil {
		return sarama.ProducerMessage{}, err
	}

	return sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: valueEncoder,
	}, nil
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

	return &avroEncoder{schema.ID(), binary}, nil
}
