package config

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/toventang/eklog/scram"
)

// DefaultProducer is used to initialize KafkaAvroConsumer
// WARNING customize at your own risk
func DefaultProducer() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond
	return config
}

// DefaultConsumer is used to initialize KafkaAvroConsumer
// WARNING customize at your own risk
func DefaultConsumer() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	return config
}

// SASLPLAIN returns a SASL/PLAIN enabled version of the given configuration
func SASLPLAIN(existing *sarama.Config, username, password string) *sarama.Config {
	var config sarama.Config
	config = *existing
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	return &config
}

// SASLSCRAMSHA256 returns a SASLSCRAMSHA256 enabled version of the given configuration
// Uses github.com/toventang/eklog/scram for SCRAMClientGeneratorFunc
func SASLSCRAMSHA256(existing *sarama.Config, username, password string) *sarama.Config {
	var config sarama.Config
	config = *existing
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &scram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA256}
	}
	return &config
}

// SASLSCRAMSHA512 returns a SASLSCRAMSHA512 enabled version of the given configuration
// Uses github.com/toventang/eklog/scram for SCRAMClientGeneratorFunc
func SASLSCRAMSHA512(existing *sarama.Config, username, password string) *sarama.Config {
	var config sarama.Config
	config = *existing
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &scram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA512}
	}
	return &config
}
