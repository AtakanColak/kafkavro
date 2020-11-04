package generator

import (
	"encoding/json"
	"strings"

	avro "github.com/actgardner/gogen-avro/v7/schema"
)

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
	nullField := avro.NewNullField("null")
	definition, _ := field.Definition(nil)
	definition["default"] = "null"

	return avro.NewField("", avro.NewUnionField("", []avro.AvroType{nullField, field.Type()}, []interface{}{map[string]interface{}{"type1": "rr"}, map[string]interface{}{"type1": "aa"}}), nil, false, nil, "", definition, 0, "")
}

// Record is a wrapper for NewRecord
func Record(name, namespace string, aliases []avro.QualifiedName, fields []*avro.Field, definition map[string]interface{}) *avro.RecordDefinition {
	if definition == nil {
		definition = make(map[string]interface{})
	}
	definition["name"] = name
	definition["type"] = "record"
	definition["namespace"] = namespace
	return avro.NewRecordDefinition(avro.QualifiedName{Name: name, Namespace: namespace}, aliases, fields, "", definition)
}

// SchemaFromRecord returns the schema in an ordered fashion
func SchemaFromRecord(record *avro.RecordDefinition) (string, error) {
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

	return marshalAvroSchema(os), nil
}
