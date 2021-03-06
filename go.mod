module github.com/AtakanColak/kafkavro

go 1.13

require (
	github.com/Shopify/sarama v1.23.1
	github.com/actgardner/gogen-avro/v7 v7.1.1
	github.com/bsm/sarama-cluster v2.1.15+incompatible
	github.com/heetch/avro v0.2.6
	github.com/linkedin/goavro v2.1.0+incompatible
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/onsi/ginkgo v1.10.2 // indirect
	github.com/onsi/gomega v1.7.0 // indirect
	github.com/riferrei/srclient v0.0.0-20200916203554-5cb7bacc8d30
	github.com/rogpeppe/gogen-avro/v7 v7.2.1
	github.com/toventang/eklog v0.0.0-20190830170558-183704f1d94a
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
)

replace github.com/riferrei/srclient => github.com/AtakanColak/srclient v0.0.0-20201103093622-a94a88c836e9
