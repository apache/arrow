// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build go1.18

package exprs

import (
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/substrait-io/substrait-go/expr"
)

func getFields(typ arrow.DataType) []arrow.Field {
	if nested, ok := typ.(arrow.NestedType); ok {
		return nested.Fields()
	}
	return nil
}

// GetRefField evaluates the substrait field reference to retrieve the
// referenced field or return an error.
func GetRefField(ref expr.ReferenceSegment, fields []arrow.Field) (*arrow.Field, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	var (
		out *arrow.Field
	)

	for ref != nil {
		if len(fields) == 0 {
			return nil, fmt.Errorf("%w: %s", compute.ErrNoChildren, out.Type)
		}

		switch f := ref.(type) {
		case *expr.StructFieldRef:
			if f.Field < 0 || f.Field >= int32(len(fields)) {
				return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, f)
			}

			out = &fields[f.Field]
			fields = getFields(out.Type)
		default:
			return nil, arrow.ErrNotImplemented
		}

		ref = ref.GetChild()
	}

	return out, nil
}

// GetRefSchema evaluates the provided substrait field reference against
// the schema to retrieve the referenced (potentially nested) field.
func GetRefSchema(ref expr.ReferenceSegment, schema *arrow.Schema) (*arrow.Field, error) {
	return GetRefField(ref, schema.Fields())
}

// GetScalar returns the evaluated referenced scalar value from the provided
// scalar which must be appropriate to the type of reference.
//
// A StructFieldRef can only reference against a Struct-type scalar, a
// ListElementRef can only reference against a List or LargeList scalar,
// and a MapKeyRef will only reference against a Map scalar. An error is
// returned if following the reference children ends up with an invalid
// nested reference object.
func GetScalar(ref expr.ReferenceSegment, s scalar.Scalar, mem memory.Allocator, ext ExtensionIDSet) (scalar.Scalar, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	var out scalar.Scalar
	for ref != nil {
		switch f := ref.(type) {
		case *expr.StructFieldRef:
			if s.DataType().ID() != arrow.STRUCT {
				return nil, fmt.Errorf("%w: attempting to reference field from non-struct scalar %s",
					arrow.ErrInvalid, s)
			}

			st := s.(*scalar.Struct)
			if f.Field < 0 || f.Field >= int32(len(st.Value)) {
				return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, ref)
			}

			out = st.Value[f.Field]
		case *expr.ListElementRef:
			switch v := s.(type) {
			case *scalar.List:
				sc, err := scalar.GetScalar(v.Value, int(f.Offset))
				if err != nil {
					return nil, err
				}
				out = sc
			case *scalar.LargeList:
				sc, err := scalar.GetScalar(v.Value, int(f.Offset))
				if err != nil {
					return nil, err
				}
				out = sc
			default:
				return nil, fmt.Errorf("%w: cannot get ListElementRef from non-list scalar %s",
					arrow.ErrInvalid, v)
			}
		case *expr.MapKeyRef:
			v, ok := s.(*scalar.Map)
			if !ok {
				return nil, arrow.ErrInvalid
			}

			dt, _, err := FromSubstraitType(f.MapKey.GetType(), ext)
			if err != nil {
				return nil, err
			}

			if !arrow.TypeEqual(dt, v.Type.(*arrow.MapType).KeyType()) {
				return nil, arrow.ErrInvalid
			}

			keyvalDatum, err := literalToDatum(mem, f.MapKey, ext)
			if err != nil {
				return nil, err
			}

			var (
				keyval      = keyvalDatum.(*compute.ScalarDatum)
				m           = v.Value.(*array.Struct)
				keys        = m.Field(0)
				valueScalar scalar.Scalar
			)
			for i := 0; i < v.Value.Len(); i++ {
				kv, err := scalar.GetScalar(keys, i)
				if err != nil {
					return nil, err
				}
				if scalar.Equals(kv, keyval.Value) {
					valueScalar, err = scalar.GetScalar(m.Field(1), i)
					if err != nil {
						return nil, err
					}
					break
				}
			}

			if valueScalar == nil {
				return nil, arrow.ErrNotFound
			}

			out = valueScalar
		}
		s = out
		ref = ref.GetChild()
	}

	return out, nil
}

// GetReferencedValue retrieves the referenced (potentially nested) value from
// the provided datum which may be a scalar, array, or record batch.
func GetReferencedValue(mem memory.Allocator, ref expr.ReferenceSegment, value compute.Datum, ext ExtensionIDSet) (compute.Datum, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	for ref != nil {
		// process the rest of the refs for the scalars
		// since arrays can go down to a scalar, but you
		// won't get an array from a scalar via ref
		if v, ok := value.(*compute.ScalarDatum); ok {
			out, err := GetScalar(ref, v.Value, mem, ext)
			if err != nil {
				return nil, err
			}

			return &compute.ScalarDatum{Value: out}, nil
		}

		switch r := ref.(type) {
		case *expr.MapKeyRef:
			return nil, arrow.ErrNotImplemented
		case *expr.StructFieldRef:
			switch v := value.(type) {
			case *compute.ArrayDatum:
				if v.Type().ID() != arrow.STRUCT {
					return nil, fmt.Errorf("%w: struct field ref for non struct type %s",
						arrow.ErrInvalid, v.Type())
				}

				if r.Field < 0 || r.Field >= int32(len(v.Value.Children())) {
					return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, ref)
				}

				value = &compute.ArrayDatum{Value: v.Value.Children()[r.Field]}
			case *compute.RecordDatum:
				if r.Field < 0 || r.Field >= int32(v.Value.NumCols()) {
					return nil, fmt.Errorf("%w: indices=%s", compute.ErrIndexRange, ref)
				}

				value = &compute.ArrayDatum{Value: v.Value.Column(int(r.Field)).Data()}
			default:
				return nil, arrow.ErrNotImplemented
			}
		case *expr.ListElementRef:
			switch v := value.(type) {
			case *compute.ArrayDatum:
				switch v.Type().ID() {
				case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
					arr := v.MakeArray()
					defer arr.Release()

					sc, err := scalar.GetScalar(arr, int(r.Offset))
					if err != nil {
						return nil, err
					}
					if s, ok := sc.(scalar.Releasable); ok {
						defer s.Release()
					}

					value = &compute.ScalarDatum{Value: sc}
				default:
					return nil, fmt.Errorf("%w: cannot reference list element in non-list array type %s",
						arrow.ErrInvalid, v.Type())
				}

			default:
				return nil, arrow.ErrNotImplemented
			}
		}

		ref = ref.GetChild()
	}

	return value, nil
}
