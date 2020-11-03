package generator_test

import (
	"testing"

	avro "github.com/actgardner/gogen-avro/v7/schema"
	"github.com/linkedin/goavro/v2"
)

type A struct {
	Name  string
	Value int64
}

func GenerateSchemaForTestTopicA() string {
	record := avro.NewRecordDefinition(avro.QualifiedName{
		Namespace: "",
		Name:      "A",
	},
		nil,
		[]*avro.Field{
			avro.NewField("Name", avro.NewStringField("string"), nil, false, nil, "", map[string]interface{}{"name": "Name"}, 0, ""),
			avro.NewField("Value", avro.NewLongField("long"), nil, false, nil, "", map[string]interface{}{"name": "Value"}, 0, ""),
		},
		"",
		map[string]interface{}{
			"type": "record",
		})

	s, err := record.Schema()
	if err != nil {
		panic(err.Error())
	}
	return s
}

func TestGenerateSchemaForTestTopicA(t *testing.T) {
	unorderedSchema := GenerateSchemaForTestTopicA()
	codec, err := goavro.NewCodec(unorderedSchema)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(codec.Schema())
}
