package generator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	avro "github.com/actgardner/gogen-avro/v7/schema"
)

var (
	timestampPreference      string = "timestamp-millis"
	timestampPreferenceMutex sync.RWMutex

	TimestampMillisDefinition = map[string]interface{}{"logicalType": "timestamp-millis"}
)

// AvroTypeName of a given primitive Go value
// struct, map and arrays are not supported
func AvroTypeName(value interface{}) (string, error) {
	var typename string
	err := fmt.Errorf("type %T is not supported by AvroTypeName", value)
	if value == nil {
		return "null", nil
	}
	t := reflect.TypeOf(value)
	switch k := t.Kind(); k {
	case reflect.Invalid:
		typename = "null"
	case reflect.Bool:
		typename = "boolean"
	case reflect.String:
		typename = "string"
	case reflect.Float32:
		typename = "float"
	case reflect.Float64:
		typename = "double"
	case reflect.Int8,
		reflect.Uint8,
		reflect.Int16,
		reflect.Uint16,
		reflect.Int32,
		reflect.Int:
		typename = "int"
	case reflect.Uint32,
		reflect.Int64,
		reflect.Uint64,
		reflect.Uint:
		typename = "long"
	case reflect.Slice:
		if t.Elem().Kind() != reflect.Uint8 {
			return "", err
		}
		typename = "bytes"
	case reflect.Struct:
		if t.PkgPath() != "time" || t.Name() != "Time" {
			return "", err
		}
		typename = "long"
	case reflect.Ptr:
		nt := reflect.New(t.Elem())
		if nt.IsNil() || !nt.CanInterface() {
			return "", err
		}
		return AvroTypeName(nt.Elem().Interface())
	default:
		return "", err
	}
	return typename, nil
}

// ChangeTimestampPreference for time.Time values
// Default is "timestamp-millis"
func ChangeTimestampPreference(preference string) {
	switch preference {
	case "timestamp-millis", "timestamp-micros", "local-timestamp-millis", "local-timestamp-micros":
		timestampPreferenceMutex.Lock()
		defer timestampPreferenceMutex.Unlock()
		timestampPreference = preference
	default: // NO-OP if invalid preference
	}
}

// AvroDefinition returns definition for logicalTypes
func AvroDefinition(value interface{}) map[string]interface{} {
	timestampPreferenceMutex.RLock()
	defer timestampPreferenceMutex.RUnlock()
	switch value.(type) {
	case time.Time:
		return map[string]interface{}{"logicalType": timestampPreference}
	default:
		return nil
	}
}

// Field is a wrapper for NewField
func Field(name, avroType string, definition map[string]interface{}) *avro.Field {
	if definition == nil {
		definition = make(map[string]interface{})
	}
	definition["name"] = name
	return avro.NewField(name, avro.NewNullField(avroType), nil, false, nil, "", definition, 0, "")
}

// MakeNullable is a wrapper to make an avro field nullable
func MakeNullable(field *avro.Field) *avro.Field {
	bufferedField := new(avro.Field)
	*bufferedField = *field

	nullField := avro.NewNullField("null")
	definition, _ := bufferedField.Definition(nil)

	// if is a logicalType, recreate the Field with logicalType embedded in it because gogen-avro doesn't support it
	if logicalTypeI, exists := definition["logicalType"]; exists {
		logicalType, ok := logicalTypeI.(string)
		if !ok {
			panic("logicalType is not provided as a string")
		}

		bufferedField = Field(field.SimpleName(), `{"type":"`+definition["type"].(string)+`","logicalType":"`+logicalType+`"}`, nil)
	}
	definition["default"] = "null"
	delete(definition, "logicalType")
	return avro.NewField("", avro.NewUnionField("", []avro.AvroType{nullField, bufferedField.Type()}, []interface{}{map[string]interface{}{"type1": "rr"}, map[string]interface{}{"type1": "aa"}}), nil, false, nil, "", definition, 0, "")
}

// RecordDefinition is a wrapper for NewRecordDefinition
func RecordDefinition(name, namespace string, aliases []avro.QualifiedName, fields []*avro.Field, definition map[string]interface{}) *avro.RecordDefinition {
	if definition == nil {
		definition = make(map[string]interface{})
	}
	definition["name"] = name
	definition["type"] = "record"
	definition["namespace"] = namespace
	return avro.NewRecordDefinition(avro.QualifiedName{Name: name, Namespace: namespace}, aliases, fields, "", definition)
}

// SchemaFromRecordDefinition returns the schema in an ordered fashion
func SchemaFromRecordDefinition(record *avro.RecordDefinition) (string, error) {
	type OrderedAvroField struct {
		Name        string          `json:"name"`
		Doc         string          `json:"doc,omitempty"`
		Type        json.RawMessage `json:"type"`
		LogicalType json.RawMessage `json:"logicalType,omitempty"`
		Default     json.RawMessage `json:"default,omitempty"`
		Order       json.RawMessage `json:"order,omitempty"`
		Aliases     json.RawMessage `json:"aliases,omitempty"`
	}

	type OrderedAvroSchema struct {
		Type      string             `json:"type"`
		Name      string             `json:"name"`
		Namespace string             `json:"namespace,omitempty"`
		Doc       string             `json:"doc,omitempty"`
		Aliases   json.RawMessage    `json:"aliases,omitempty"`
		Fields    []OrderedAvroField `json:"fields"`
	}

	addRawMessageIfNotEmpty := func(name string, msg json.RawMessage, end string) string {
		if msg == nil {
			return ""
		}
		return `"` + name + `":` + string(msg) + end
	}

	addStringIfNotEmpty := func(name, value, end string) string {
		if value == "" {
			return ""
		}
		return addRawMessageIfNotEmpty(name, json.RawMessage(`"`+value+`"`), end)
	}

	marshalAvroField := func(of OrderedAvroField) string {
		// there is an escape issue with logical types, this is to fix that
		typeString := strings.ReplaceAll(string(of.Type), `\"`, `"`)
		typeString = strings.ReplaceAll(typeString, `"{"`, `{"`)
		typeString = strings.ReplaceAll(typeString, `"}"`, `"}`)

		s := `{"name":"` + of.Name + `",`
		s += addStringIfNotEmpty("doc", of.Doc, ",")
		s += `"type":` + typeString + `,`
		s += addRawMessageIfNotEmpty("logicalType", of.LogicalType, ",")
		s += addRawMessageIfNotEmpty("default", of.Default, ",")
		s += addRawMessageIfNotEmpty("order", of.Order, ",")
		s += addRawMessageIfNotEmpty("aliases", of.Aliases, "")
		if s[len(s)-1] == ',' {
			s = s[:len(s)-1]
		}
		s += "}"
		return s
	}

	marshalAvroFields := func(ofs []OrderedAvroField) string {
		var sb strings.Builder
		sb.WriteString("[")
		for index, of := range ofs {
			sb.WriteString(marshalAvroField(of))
			if index < len(ofs)-1 {
				sb.WriteString(",")
			}
		}
		sb.WriteString("]")
		return sb.String()
	}

	marshalAvroSchema := func(os *OrderedAvroSchema) string {
		s := `{"type":"` + os.Type + `","name":"` + os.Name + `",`
		s += addStringIfNotEmpty("namespace", os.Namespace, ",")
		s += addStringIfNotEmpty("doc", os.Doc, ",")
		s += addRawMessageIfNotEmpty("aliases", os.Aliases, ",")
		s += `"fields":` + marshalAvroFields(os.Fields) + `}`
		return s
	}

	os := new(OrderedAvroSchema)
	schema, err := record.Schema()
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal([]byte(schema), os); err != nil {
		return "", err
	}

	// to avoid namespace mix
	qualifiedName := record.AvroName()
	os.Name = qualifiedName.Name
	os.Namespace = qualifiedName.Namespace

	return marshalAvroSchema(os), nil
}

// FieldsFromMap generates a Fields array from given map
func FieldsFromMap(m map[string]interface{}) ([]*avro.Field, error) {
	fields := make([]*avro.Field, 0)
	for key, val := range m {
		avroType, err := AvroTypeName(val)
		if err != nil {
			return nil, err
		}
		definition := AvroDefinition(val)
		fields = append(fields, Field(key, avroType, definition))
	}
	return fields, nil
}

// RecordDefinitionFromMap is a wrapper utility function that generates a RecordDefinition from given map value
// By default all fields are not nullable, for them to be nullable, you can either do it by hand or call MakeAllFieldsNullable
func RecordDefinitionFromMap(name, namespace string, value map[string]interface{}) (*avro.RecordDefinition, error) {
	fields, err := FieldsFromMap(value)
	if err != nil {
		return nil, err
	}
	return RecordDefinition(name, namespace, nil, fields, nil), nil
}

// MakeAllFieldsNullable for a given record, with no side effects
func MakeAllFieldsNullable(record *avro.RecordDefinition) *avro.RecordDefinition {
	fields := record.Fields()
	for i := range fields {
		fields[i] = MakeNullable(fields[i])
	}

	definitionI, _ := record.Definition(map[avro.QualifiedName]interface{}{})
	var definition map[string]interface{}
	if definitionAsMap, isMap := definitionI.(map[string]interface{}); isMap {
		definition = definitionAsMap
	}

	return avro.NewRecordDefinition(record.AvroName(), record.Aliases(), fields, record.Doc(), definition)
}
