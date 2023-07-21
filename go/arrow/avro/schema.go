// Package avro reads Avro OCF files and presents the extracted data as records
package avro

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow/go/v13/arrow"
)

type schemaNode struct {
	name        string
	ofType      interface{}
	logicalType string
	precision   int32
	scale       int32
	size        int
	symbols     []string
	fields      []interface{}
}

// ArrowSchemaFromAvro returns a new Arrow schema from an Avro schema JSON.
// If the top level is of record type, set includeTopLevel to either make
// its fields top level fields in the resulting schema or nested in a single field.
func ArrowSchemaFromAvro(avroSchema []byte, includeTopLevel bool) (*arrow.Schema, error) {
	var m map[string]interface{}
	var node schemaNode
	json.Unmarshal(avroSchema, &m)
	if includeTopLevel {
		if n, ok := m["name"]; ok {
			node.name = n.(string)
		} else {
			return nil, fmt.Errorf("invalid avro schema: no top level record name found")
		}
		node.fields = append(node.fields, m)
	} else {
		if m["type"].(string) == "record" {
			if _, ok := m["fields"]; ok {
				for _, field := range m["fields"].([]interface{}) {
					node.fields = append(node.fields, field.(map[string]interface{}))
				}
				if len(node.fields) == 0 {
					return nil, fmt.Errorf("invalid avro schema: no top level record fields found")
				}
			}
		}
	}

	fields := iterateFields(node.fields)
	return arrow.NewSchema(fields, nil), nil
}

func iterateFields(f []interface{}) []arrow.Field {
	var s []arrow.Field
	for _, field := range f {
		var n schemaNode
		n.name = field.(map[string]interface{})["name"].(string)
		n.ofType = field.(map[string]interface{})["type"]
		switch n.ofType.(type) {
		// Getting field type from within field object
		case string:
			switch n.ofType.(string) {
			case "enum":
				for _, symbol := range field.(map[string]interface{})["symbols"].([]interface{}) {
					n.symbols = append(n.symbols, symbol.(string))
				}
			default:
				if lt, ok := field.(map[string]interface{})["logicalType"]; ok {
					n.logicalType = lt.(string)
				}
				if lt, ok := field.(map[string]interface{})["size"]; ok {
					n.size = int(lt.(float64))
				}
				if lt, ok := field.(map[string]interface{})["precision"]; ok {
					n.precision = int32(lt.(float64))
				}
				if lt, ok := field.(map[string]interface{})["scale"]; ok {
					n.scale = int32(lt.(float64))
				}
			}
		// Field type is an object
		case map[string]interface{}:
			if lt, ok := field.(map[string]interface{})["type"].(map[string]interface{})["logicalType"]; ok {
				n.logicalType = lt.(string)
			}
			if lt, ok := field.(map[string]interface{})["type"].(map[string]interface{})["size"]; ok {
				n.size = int(lt.(float64))
			}
			if lt, ok := field.(map[string]interface{})["type"].(map[string]interface{})["precision"]; ok {
				n.precision = int32(lt.(float64))
			}
			if lt, ok := field.(map[string]interface{})["type"].(map[string]interface{})["scale"]; ok {
				n.scale = int32(lt.(float64))
			}
		case []interface{}:

		default:
			if lt, ok := field.(map[string]interface{})["logicalType"]; ok {
				n.logicalType = lt.(string)
			}
			if lt, ok := field.(map[string]interface{})["size"]; ok {
				n.size = int(lt.(float64))
			}
			if lt, ok := field.(map[string]interface{})["precision"]; ok {
				n.precision = int32(lt.(float64))
			}
			if lt, ok := field.(map[string]interface{})["scale"]; ok {
				n.scale = int32(lt.(float64))
			}
		}
		// Field is of type "record"
		if nf, f := field.(map[string]interface{})["fields"]; f {
			switch nf.(type) {
			// primitive & complex types
			case map[string]interface{}:
				for _, v := range nf.(map[string]interface{})["fields"].([]interface{}) {
					n.fields = append(n.fields, v.(map[string]interface{}))
				}
			// type unions
			default:
				for _, v := range nf.([]interface{}) {
					n.fields = append(n.fields, v.(map[string]interface{}))
				}
			}
		}
		s = append(s, traverseNodes(n))
	}
	return s
}

func traverseNodes(node schemaNode) arrow.Field {
	switch node.ofType.(type) {
	case string:
		// Avro primitive type
		if len(node.fields) == 0 {
			switch node.ofType.(string) {
			case "boolean", "int", "long", "float", "double", "bytes", "string":
				if node.logicalType != "" {
					return avroLogicalToArrowField(node)
				}
				//  Avro primitive type
				return arrow.Field{Name: node.name, Type: AvroPrimitiveToArrowType(node.ofType.(string))}

			case "fixed":
				if node.logicalType == "duration" {
					return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.MonthDayNanoInterval}
				}
				return arrow.Field{Name: node.name, Type: &arrow.FixedSizeBinaryType{ByteWidth: node.size}}
			case "enum":
				symbols := make(map[string]string)
				for index, symbol := range node.symbols {
					k := strconv.FormatInt(int64(index), 10)
					symbols[k] = symbol
				}
				var dt arrow.DictionaryType = arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: arrow.BinaryTypes.String, Ordered: false}
				sl := len(symbols)
				switch {
				case sl <= math.MaxUint8:
					dt.IndexType = arrow.PrimitiveTypes.Uint8
				case sl > math.MaxUint8 && sl <= math.MaxUint16:
					dt.IndexType = arrow.PrimitiveTypes.Uint16
				case sl > math.MaxUint16 && sl <= math.MaxUint32:
					dt.IndexType = arrow.PrimitiveTypes.Uint32
				}
				return arrow.Field{Name: node.name, Type: &dt, Nullable: true, Metadata: arrow.MetadataFrom(symbols)}
			default:
				return arrow.Field{Name: node.name, Type: AvroPrimitiveToArrowType(node.ofType.(string))}
			}
		} else {
			// avro "record" type, node has "fields" array
			if node.ofType.(string) == "record" {
				var n schemaNode
				n.name = node.name
				n.ofType = node.ofType
				if len(node.fields) > 0 {
					n.fields = append(n.fields, node.fields...)
				}
				f := iterateFields(n.fields)
				return arrow.Field{Name: node.name, Type: arrow.StructOf(f...)}
			}
		}
	// Avro complex types
	case map[string]interface{}:
		//var n schemaNode
		//n.name = node.name
		//n.ofType = node.ofType.(map[string]interface{})["type"]
		switch node.logicalType {
		case "":
			return avroComplexToArrowField(node)
		default:
			return avroLogicalToArrowField(node)
		}

	// Avro union types
	case []interface{}:
		var unionTypes []string
		for _, ft := range node.ofType.([]interface{}) {
			switch ft.(type) {
			// primitive types
			case string:
				if ft != "null" {
					unionTypes = append(unionTypes, ft.(string))
				}
				continue
			// complex types
			case map[string]interface{}:
				var n schemaNode
				n.name = node.name
				n.ofType = ft.(map[string]interface{})["type"]
				if _, f := ft.(map[string]interface{})["fields"]; f {
					for _, field := range ft.(map[string]interface{})["fields"].([]interface{}) {
						n.fields = append(n.fields, field.(map[string]interface{}))
					}
				}
				f := iterateFields(n.fields)
				return arrow.Field{Name: node.name, Type: arrow.StructOf(f...)}
			}
		}
		// Supported Avro union type is null + one other type.
		// TODO: Complex AVRO union to Arrow Dense || Sparse Union.
		if len(unionTypes) == 1 {
			return arrow.Field{Name: node.name, Type: AvroPrimitiveToArrowType(unionTypes[0])}
		} else {
			// BYTE_ARRAY is the catchall if union type is anything beyond null + one other type.
			return arrow.Field{Name: node.name, Type: arrow.BinaryTypes.Binary}
		}
	}
	return arrow.Field{Name: node.name, Type: arrow.BinaryTypes.Binary}
}

func avroLogicalToArrowField(node schemaNode) arrow.Field {
	// Avro logical types
	switch node.logicalType {
	// The decimal logical type represents an arbitrary-precision signed decimal number of the form unscaled × 10-scale.
	// A decimal logical type annotates Avro bytes or fixed types. The byte array must contain the two’s-complement
	// representation of the unscaled integer value in big-endian byte order. The scale is fixed, and is specified
	// using an attribute.
	//
	// The following attributes are supported:
	// scale, a JSON integer representing the scale (optional). If not specified the scale is 0.
	// precision, a JSON integer representing the (maximum) precision of decimals stored in this type (required).
	case "decimal":
		return arrow.Field{Name: node.name, Type: &arrow.Decimal128Type{Precision: node.precision, Scale: node.scale}}
	// The uuid logical type represents a random generated universally unique identifier (UUID).
	// A uuid logical type annotates an Avro string. The string has to conform with RFC-4122
	case "uuid":
		return arrow.Field{Name: node.name, Type: arrow.BinaryTypes.String}

	// The date logical type represents a date within the calendar, with no reference to a particular
	// time zone or time of day.
	// A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch,
	// 1 January 1970 (ISO calendar).
	case "date":
		return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Date32}

	// The time-millis logical type represents a time of day, with no reference to a particular calendar,
	// time zone or date, with a precision of one millisecond.
	// A time-millis logical type annotates an Avro int, where the int stores the number of milliseconds
	// after midnight, 00:00:00.000.
	case "time-millis":
		return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Time32ms}

	// The time-micros logical type represents a time of day, with no reference to a particular calendar,
	// time zone or date, with a precision of one microsecond.
	// A time-micros logical type annotates an Avro long, where the long stores the number of microseconds
	// after midnight, 00:00:00.000000.
	case "time-micros":
		return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Time64us}

	// The timestamp-millis logical type represents an instant on the global timeline, independent of a
	// particular time zone or calendar, with a precision of one millisecond. Please note that time zone
	// information gets lost in this process. Upon reading a value back, we can only reconstruct the instant,
	// but not the original representation. In practice, such timestamps are typically displayed to users in
	// their local time zones, therefore they may be displayed differently depending on the execution environment.
	// A timestamp-millis logical type annotates an Avro long, where the long stores the number of milliseconds
	// from the unix epoch, 1 January 1970 00:00:00.000 UTC.
	case "timestamp-millis":
		return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Timestamp_ms}
	// The timestamp-micros logical type represents an instant on the global timeline, independent of a
	// particular time zone or calendar, with a precision of one microsecond. Please note that time zone
	// information gets lost in this process. Upon reading a value back, we can only reconstruct the instant,
	// but not the original representation. In practice, such timestamps are typically displayed to users
	// in their local time zones, therefore they may be displayed differently depending on the execution environment.
	// A timestamp-micros logical type annotates an Avro long, where the long stores the number of microseconds
	// from the unix epoch, 1 January 1970 00:00:00.000000 UTC.
	case "timestamp-micros":
		return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Timestamp_us}
	// The local-timestamp-millis logical type represents a timestamp in a local timezone, regardless of
	// what specific time zone is considered local, with a precision of one millisecond.
	// A local-timestamp-millis logical type annotates an Avro long, where the long stores the number of
	// milliseconds, from 1 January 1970 00:00:00.000.
	case "local-timestamp-millis":
		return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Timestamp_ms}
	// The local-timestamp-micros logical type represents a timestamp in a local timezone, regardless of
	// what specific time zone is considered local, with a precision of one microsecond.
	// A local-timestamp-micros logical type annotates an Avro long, where the long stores the number of
	// microseconds, from 1 January 1970 00:00:00.000000.
	case "local-timestamp-micros":
		return arrow.Field{Name: node.name, Type: arrow.FixedWidthTypes.Timestamp_us}
	//  Avro primitive type
	default:
		return arrow.Field{Name: node.name, Type: AvroPrimitiveToArrowType(node.ofType.(string))}
	}
	return arrow.Field{}
}

func avroComplexToArrowField(node schemaNode) arrow.Field {
	var n schemaNode
	n.name = node.name
	n.ofType = node.ofType.(map[string]interface{})["type"]
	// Avro "array" field type
	if i, ok := node.ofType.(map[string]interface{})["items"]; ok {
		return arrow.Field{Name: node.name, Type: arrow.ListOf(AvroPrimitiveToArrowType(i.(string)))}
	}
	// Avro "enum" field type = Arrow dictionary type
	if i, ok := node.ofType.(map[string]interface{})["symbols"]; ok {
		symbols := make(map[string]string)
		for index, symbol := range i.([]interface{}) {
			k := strconv.FormatInt(int64(index), 10)
			symbols[k] = symbol.(string)
		}
		var dt arrow.DictionaryType = arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Uint64, ValueType: arrow.BinaryTypes.String, Ordered: false}
		sl := len(symbols)
		switch {
		case sl <= math.MaxUint8:
			dt.IndexType = arrow.PrimitiveTypes.Uint8
		case sl > math.MaxUint8 && sl <= math.MaxUint16:
			dt.IndexType = arrow.PrimitiveTypes.Uint16
		case sl > math.MaxUint16 && sl <= math.MaxUint32:
			dt.IndexType = arrow.PrimitiveTypes.Uint32
		}
		return arrow.Field{Name: node.name, Type: &dt, Nullable: true, Metadata: arrow.MetadataFrom(symbols)}
	}
	// Avro "fixed" field type = Arrow FixedSize Primitive BinaryType
	if i, ok := node.ofType.(map[string]interface{})["size"]; ok {
		return arrow.Field{Name: node.name, Type: &arrow.FixedSizeBinaryType{ByteWidth: int(i.(float64))}}
	}
	// Avro "map" field type
	if i, ok := node.ofType.(map[string]interface{})["values"]; ok {
		return arrow.Field{Name: node.name, Type: arrow.MapOf(arrow.BinaryTypes.String, AvroPrimitiveToArrowType(i.(string)))}
	}
	// Avro "record" field type
	if _, f := node.ofType.(map[string]interface{})["fields"]; f {
		for _, field := range node.ofType.(map[string]interface{})["fields"].([]interface{}) {
			n.fields = append(n.fields, field.(map[string]interface{}))
		}
		s := iterateFields(n.fields)
		return arrow.Field{Name: n.name, Type: arrow.StructOf(s...)}
	}
	return arrow.Field{}
}

// AvroPrimitiveToArrowType returns the Arrow DataType equivalent to a
// Avro primitive type.
//
// NOTE: Arrow Binary type is used as a catchall to avoid potential data loss.
func AvroPrimitiveToArrowType(avroFieldType string) arrow.DataType {
	switch avroFieldType {
	// int: 32-bit signed integer
	case "int":
		return arrow.PrimitiveTypes.Int32
	// long: 64-bit signed integer
	case "long":
		return arrow.PrimitiveTypes.Int64
	// float: single precision (32-bit) IEEE 754 floating-point number
	case "float":
		return arrow.PrimitiveTypes.Float32
	// double: double precision (64-bit) IEEE 754 floating-point number
	case "double":
		return arrow.PrimitiveTypes.Float64
	// bytes: sequence of 8-bit unsigned bytes
	case "bytes":
		return &arrow.FixedSizeBinaryType{ByteWidth: 8}
	// boolean: a binary value
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	// string: unicode character sequence
	case "string":
		return arrow.BinaryTypes.String
	// fallback to binary type for any unsupported type
	default:
		return arrow.BinaryTypes.Binary
	}
}
