package producer_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/AtakanColak/kafkavro/config"
	"github.com/AtakanColak/kafkavro/producer"
	"github.com/Shopify/sarama/mocks"
	"github.com/riferrei/srclient"
)

var (
	schemaRegistryClient = srclient.CreateMockSchemaRegistryClient("https://schema.registry.com:8081")

	predefMessageData = map[string]struct{string, []byte, []byte, *srclient.Schema, *srclient.Schema}{}
)

func init() {

}

func initializeSchemaRegistryClient(t *testing.T) srclient.initializeSchemaRegistryClient {
	var (
		url    = "https://schema.registry.com:8081"
		client = srclient.CreateMockSchemaRegistryClient(url)
	)

	return client
}

func valueCheckerEquals(ref []byte) mocks.ValueChecker {
	return func(val []byte) error {
		if !bytes.Equal(ref, val) {
			return fmt.Errorf("expected '%s', got '%s'", ref, val)
		}
		return nil
	}
}

type kafkaAvroSyncProducerTestSetup struct {
	keySchema   *srclient.Schema
	valueSchema *srclient.Schema
	key         []byte
	value       []byte
}

func TestSendMessage(t *testing.T) {
	var (
		topic                = "topic_TestSentMessage"
		key                  = []byte("key_TestSentMessage")
		value                = []byte("value_TestSendMessage")
		syncProducer         = mocks.NewSyncProducer(t, config.DefaultProducer())
		schemaRegistryClient = initializeSchemaRegistryClient(t)
	)
	defer syncProducer.Close()
	avroSyncProducer, err := producer.NewKafkaAvroSyncProducer(100, syncProducer, schemaRegistryClient)
	if err != nil {
		t.Fatal(err.Error())
	}
	msg, err := producer.PrepareProducerMessage(topic, nil, nil, key, value, false, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	syncProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(valueCheckerEquals(value))
	avroSyncProducer.SendMessage(&msg)
}

func TestSendMessages(t *testing.T) {
	var (
		topic                = "topic_TestSentMessages"
		key                  = []byte("key_TestSentMessages")
		value                = []byte("value_TestSendMessages")
		syncProducer         = mocks.NewSyncProducer(t, config.DefaultProducer())
		schemaRegistryClient = initializeSchemaRegistryClient(t)
	)
}
