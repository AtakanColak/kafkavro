package generator_test

import (
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"

	avro "github.com/actgardner/gogen-avro/v7/schema"

	"github.com/AtakanColak/kafkavro/generator"
)

// reference
type A struct {
	Name  string `avrogen:"nullable"`
	Value int64
}

func (a A) ToMap() (map[string]interface{}, error) {
	return map[string]interface{}{
		"Name":  a.Name,
		"Value": a.Value,
	}, nil
}

type B struct {
	Time       time.Time
	Host       string
	AgentUUID  string
	MemoryFree int64
	PID        int
	CPUPercent float64
}

func (b B) ToMap() (map[string]interface{}, error) {
	return map[string]interface{}{
		"Time":       b.Time,
		"Host":       b.Host,
		"AgentUUID":  b.AgentUUID,
		"MemoryFree": b.MemoryFree,
		"PID":        b.PID,
		"CPUPercent": b.CPUPercent,
	}, nil
}

var (
	expectedSchemaA = `{"type":"record","name":"A","namespace":"com.testing","fields":[{"name":"Name","type":"string"},{"name":"Value","type":"long"}]}`
)

func checkSchema(expected, schema string, t testing.TB) {
	if schema != expected {
		t.Fatalf("expected:\n%s\nbut got:\n%s", expected, schema)
	}

	if _, err := goavro.NewCodec(schema); err != nil {
		t.Fatalf("generated schema can't be parsed: %s", err.Error())
	}
}

func TestSchemaFromRecordDefinition(t *testing.T) {
	fields := []*avro.Field{
		generator.Field("fieldA", "string", nil),
		generator.Field("fieldB", "long", generator.TimestampMillisDefinition),
		generator.MakeNullable(generator.Field("fieldC", "int", nil)),
	}

	record := generator.RecordDefinition("test", "com.testing", nil, fields, nil)
	expected := `{"type":"record","name":"test","namespace":"com.testing","fields":[{"name":"fieldA","type":"string"},{"name":"fieldB","type":"long","logicalType":"timestamp-millis"},{"name":"fieldC","type":["null","int"],"default":"null"}]}`

	schema, err := generator.SchemaFromRecordDefinition(record)
	if err != nil {
		t.Fatal(err.Error())
	}

	checkSchema(expected, schema, t)
}

func TestMakeAllFieldsNullable(t *testing.T) {
	a := A{
		Name:  "Ali",
		Value: 23,
	}

	mapped, _ := a.ToMap()
	record, err := generator.RecordDefinitionFromMap("A", "com.testing", mapped)
	if err != nil {
		t.Fatal(err.Error())
	}
	record = generator.MakeAllFieldsNullable(record)
	t.Log(record.Schema())
}

// func TestRecordDefinitionFrom(t *testing.T) {
// 	a := A{
// 		Name:  "Ali",
// 		Value: 23,
// 	}

// 	b := B{
// 		Time:       time.Now(),
// 		Host:       "centos",
// 		AgentUUID:  "0-jgue--d",
// 		MemoryFree: 5,
// 		PID:        1,
// 		CPUPercent: 5.0,
// 	}

// 	_, _ = a, b

// 	t.Run("Map", func(t *testing.T) {
// 		runTestRecordDefinitionFromMapCase := func(name, namespace, expected string, m generator.Mappable) func(*testing.T) {
// 			return func(t *testing.T) {
// 				mapped, err := m.ToMap()
// 				if err != nil {
// 					t.Fatal(err.Error())
// 				}
// 				record, err := generator.RecordDefinitionFromMap(name, namespace, mapped)
// 				if err != nil {
// 					t.Fatal(err.Error())
// 				}
// 				schema, err := generator.SchemaFromRecordDefinition(record)
// 				if err != nil {
// 					t.Fatal(err.Error())
// 				}

// 				checkSchema(expected, schema, t)
// 			}

// 		}
// 		t.Run("A", runTestRecordDefinitionFromMapCase("A", "com.testing", "", a))
// 		t.Run("B", runTestRecordDefinitionFromMapCase("B", "com.testing", "", b))
// 	})
// }

// func TestSchemaFromStruct(t *testing.T) {
// 	schema, err := generator.SchemaFromStruct(A{})
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	checkSchema(expectedSchemaA, schema, t)
// }

// func TestRecordFromMappable(t *testing.T) {

// 	record, err := generator.RecordDefinitionFromMappable("B", "com.test.package", b)
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}

// 	schema, err := generator.SchemaFromRecordDefinition(record)
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	t.Log(schema)
// }

// func GenerateSchemaForTestTopicA() string {
// 	record := avro.NewRecordDefinition(avro.QualifiedName{
// 		Namespace: "",
// 		Name:      "A",
// 	},
// 		nil,
// 		[]*avro.Field{
// 			avro.NewField("Name", avro.NewStringField("string"), nil, false, nil, "", map[string]interface{}{"name": "Name"}, 0, ""),
// 			avro.NewField("Value", avro.NewLongField("long"), nil, false, nil, "", map[string]interface{}{"name": "Value"}, 0, ""),
// 		},
// 		"",
// 		map[string]interface{}{
// 			"type": "record",
// 		})

// 	s, err := record.Schema()
// 	if err != nil {
// 		panic(err.Error())
// 	}
// 	return s
// }

// func recordDefinitionForB() *avro.RecordDefinition {
// 	fields := []*avro.Field{
// 		generator.Field("time", "long", map[string]interface{}{"logicalType": "timestamp-millis"}),
// 		generator.Field("host", "string", nil),
// 		generator.Field("agent_uuid", "string", nil),
// 		generator.Field("memory_free", "long", nil),
// 		generator.Field("PID", "int", nil),
// 		generator.Field("cpu_percent", "double", nil),
// 	}
// 	for i := 0; i < len(fields); i++ {
// 		fields[i] = generator.MakeNullable(fields[i])
// 	}

// 	return generator.Record("B", "com_test_mytest", nil, fields, nil)
// }

// func GenerateSchemaForTestTopicB() string {
// 	record := recordDefinitionForB()
// 	s, err := record.Schema()
// 	if err != nil {
// 		panic(err.Error())
// 	}
// 	return s
// }

// func TestGenerateSchemaForTestTopicA(t *testing.T) {
// 	unorderedSchema := GenerateSchemaForTestTopicA()

// 	t.Log(unorderedSchema)
// }

// func TestGenerateSchemaForTestTopicB(t *testing.T) {
// 	t.Log(GenerateSchemaForTestTopicB())
// }

// func TestSchemaFromRecord(t *testing.T) {
// 	record := recordDefinitionForB()
// 	s, err := generator.SchemaFromRecord(record)
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	t.Log(s)
// }
// func TestDefinition(t *testing.T) {
// 	field := avro.NewField("time", avro.NewNullField("long"), nil, false, nil, "", map[string]interface{}{"name": "time", "logicalType": "timestamp-millis"}, 0, "")
// 	def, err := field.Definition(nil)
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
// 	t.Log(def)
// }
