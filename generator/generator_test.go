package generator_test

import (
	"testing"
	"time"

	avro "github.com/actgardner/gogen-avro/v7/schema"

	"github.com/AtakanColak/kafkavro/generator"
)

// reference
type A struct {
	Name  string
	Value int64
}

type B struct {
	Time       time.Time
	Host       string
	AgentUUID  string
	MemoryFree int64
	PID        int
	CPUPercent float64
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

func recordDefinitionForB() *avro.RecordDefinition {
	fields := []*avro.Field{
		generator.Field("time", `{"type":"long","logicalType":"timestamp-millis"}`, nil),
		generator.Field("host", "string", nil),
		generator.Field("agent_uuid", "string", nil),
		generator.Field("memory_free", "long", nil),
		generator.Field("PID", "int", nil),
		generator.Field("cpu_percent", "double", nil),
	}
	for i := 0; i < len(fields); i++ {
		fields[i] = generator.MakeNullable(fields[i])
	}

	return generator.Record("B", "com_test_mytest", nil, fields, nil)
}

func GenerateSchemaForTestTopicB() string {
	record := recordDefinitionForB()
	s, err := record.Schema()
	if err != nil {
		panic(err.Error())
	}
	return s
}

func TestGenerateSchemaForTestTopicA(t *testing.T) {
	unorderedSchema := GenerateSchemaForTestTopicA()

	t.Log(codec.Schema())
}

func TestGenerateSchemaForTestTopicB(t *testing.T) {
	t.Log(GenerateSchemaForTestTopicB())
}

func TestSchemaFromRecord(t *testing.T) {
	record := recordDefinitionForB()
	s, err := generator.SchemaFromRecord(record)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(s)
}
func TestDefinition(t *testing.T) {
	field := avro.NewField("time", avro.NewNullField("long"), nil, false, nil, "", map[string]interface{}{"name": "time", "logicalType": "timestamp-millis"}, 0, "")
	def, err := field.Definition(nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(def)
}
