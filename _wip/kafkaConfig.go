package kafkavro

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/toventang/eklog/scram"
)

// ProducerConfigDefault is used to initialize KafkaAvroConsumer
// WARNING customize at your own risk
func ProducerConfigDefault() *sarama.Config {
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

// ConsumerConfigDefault is used to initialize KafkaAvroConsumer
// WARNING customize at your own risk
func ConsumerConfigDefault() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	return config
}

// ConfigSASLPLAIN returns a SASL/PLAIN enabled version of the given configuration
func ConfigSASLPLAIN(existing *sarama.Config, username, password string) *sarama.Config {
	var config sarama.Config
	config = *existing
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	return &config
}

// ConfigSASLSCRAMSHA256 returns a SASLSCRAMSHA256 enabled version of the given configuration
// Uses github.com/toventang/eklog/scram for SCRAMClientGeneratorFunc
func ConfigSASLSCRAMSHA256(existing *sarama.Config, username, password string) *sarama.Config {
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

// ConfigSASLSCRAMSHA512 returns a SASLSCRAMSHA512 enabled version of the given configuration
// Uses github.com/toventang/eklog/scram for SCRAMClientGeneratorFunc
func ConfigSASLSCRAMSHA512(existing *sarama.Config, username, password string) *sarama.Config {
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
