package producer_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/riferrei/srclient"

	"github.com/AtakanColak/kafkavro/config"
	"github.com/AtakanColak/kafkavro/producer"
)

var (
	testTopic = "topic_TestTopic"
	testKey   = []byte("key_TestKey")
	testValue = []byte("value_TestValue")

	testTopicA = "topic_TestTopicA"

	mockSchemaRegistryClient = srclient.CreateMockSchemaRegistryClient("mock://url")

	schemaA = `{"fields":[{"name":"Name","type":"string"},{"name":"Value","type":"long"}],"name":"A","type":"record"}`

	schemaB = ``
)

func init() {
	_, err := mockSchemaRegistryClient.CreateSchema(testTopicA, schemaA, srclient.Avro, false)
	if err != nil {
		panic(err.Error())
	}
}

func initDefaultProducer(t testing.TB) (*producer.KafkaAvroSyncProducer, *mocks.SyncProducer) {
	mockProducer := mocks.NewSyncProducer(t, config.DefaultProducer())
	avroSyncProducer, err := producer.NewKafkaAvroSyncProducer(100, mockProducer, mockSchemaRegistryClient)
	if err != nil {
		t.Fatal(err.Error())
	}
	return avroSyncProducer, mockProducer
}

// PrepareProducerMessage is a wrapper for producer.PrepareProducerMessage for testing
func PrepareProducerMessage(t testing.TB, testTopic string, keySchema, valueSchema *srclient.Schema, testKey, testValue []byte) sarama.ProducerMessage {
	var keyIsAvro, valueIsAvro bool
	if keySchema != nil {
		keyIsAvro = true
	}
	if valueSchema != nil {
		valueIsAvro = true
	}

	msg, err := producer.PrepareProducerMessage(testTopic, keySchema, valueSchema, testKey, testValue, keyIsAvro, valueIsAvro)
	if err != nil {
		t.Fatal(err.Error())
	}
	return msg
}

func valueCheckerPlainEquals(ref []byte) mocks.ValueChecker {
	return func(val []byte) error {
		if !bytes.Equal(ref, val) {
			return fmt.Errorf("expected '%s', got '%s'", ref, val)
		}
		return nil
	}
}

func TestSendMessage(t *testing.T) {
	avroSyncProducer, mockProducer := initDefaultProducer(t)
	defer mockProducer.Close()
	mockProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(valueCheckerPlainEquals(testValue))
	msg := PrepareProducerMessage(t, testTopic, nil, nil, testKey, testValue)
	avroSyncProducer.SendMessage(&msg)
}

func TestSendMessages(t *testing.T) {
	avroSyncProducer, mockProducer := initDefaultProducer(t)
	defer mockProducer.Close()
	length := 35
	msgs := make([]*sarama.ProducerMessage, length)
	for i := 0; i < length; i++ {
		mockProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(valueCheckerPlainEquals(testValue))
		msg := PrepareProducerMessage(t, testTopic, nil, nil, testKey, testValue)
		msgs[i] = &msg
	}

	avroSyncProducer.SendMessages(msgs)
}

func TestGetSchemas(t *testing.T) {
	avroSyncProducer, _ := initDefaultProducer(t)
	_, valueSchema, err := avroSyncProducer.GetSchemas(testTopicA, false, true)
	if err != nil {
		t.Fatal(err.Error())
	}
	if valueSchema.Schema() != schemaA {
		t.Fatal("schemas are not equal")
	}
}
