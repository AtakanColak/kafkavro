package generator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	avro "github.com/actgardner/gogen-avro/v7/schema"
)

var (
	fullDef                         = make(map[avro.QualifiedName]interface{})
	timestampPreference      string = "timestamp-millis"
	timestampPreferenceMutex sync.RWMutex
)

// TypeDefinition of a given primitive Go value
// struct, map and arrays are not supported
func TypeDefinition(value interface{}) (interface{}, error) {
	err := fmt.Errorf("type %T is not supported by AvroTypeName", value)
	if value == nil {
		return "null", nil
	}
	t := reflect.TypeOf(value)
	switch k := t.Kind(); k {
	case reflect.Invalid:
		return "null", err
	case reflect.Bool:
		return "boolean", nil
	case reflect.String:
		return "string", nil
	case reflect.Float32:
		return "float", nil
	case reflect.Float64:
		return "double", nil
	case reflect.Int8,
		reflect.Uint8,
		reflect.Int16,
		reflect.Uint16,
		reflect.Int32,
		reflect.Int:
		return "int", nil
	case reflect.Uint32,
		reflect.Int64,
		reflect.Uint64,
		reflect.Uint:
		return "long", nil
	case reflect.Slice:
		if t.Elem().Kind() != reflect.Uint8 {
			return "", err
		}
		return "bytes", nil
	case reflect.Struct:
		if t.PkgPath() != "time" || t.Name() != "Time" {
			return "", err
		}
		return map[string]interface{}{"type": "long", "logicalType": timestampPreference}, nil
	case reflect.Ptr:
		nt := reflect.New(t.Elem())
		if nt.IsNil() || !nt.CanInterface() {
			return "", err
		}
		return TypeDefinition(nt.Elem().Interface())
	default:
		return "", err
	}
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

// Field is a wrapper for NewField
func Field(name string, typeDefinition interface{}, fieldDefinition map[string]interface{}) *avro.Field {
	if fieldDefinition == nil {
		fieldDefinition = make(map[string]interface{})
	}
	fieldDefinition["name"] = name
	return avro.NewField("", avro.NewNullField(typeDefinition), nil, false, nil, "", fieldDefinition, 0, "")
}

// MakeNullable is a wrapper to make an avro field nullable
func MakeNullable(field *avro.Field) *avro.Field {
	nullField := avro.NewNullField("null")
	definition, _ := field.Definition(fullDef)
	union := avro.NewUnionField("", []avro.AvroType{nullField, field.Type()}, []interface{}{"", ""})
	definition["default"] = nil
	return avro.NewField("", union, nil, false, nil, "", definition, 0, "")
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
		// typeString := FixStringEscapeInLogicalType(string(of.Type))

		s := `{"name":"` + of.Name + `",`
		s += addStringIfNotEmpty("doc", of.Doc, ",")
		s += `"type":` + string(of.Type) + `,`
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
		avroTypeDef, err := TypeDefinition(val)
		if err != nil {
			return nil, err
		}
		fields = append(fields, Field(key, avroTypeDef, nil))
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
