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
	"hash"
	"hash/maphash"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/substrait-io/substrait-go/expr"
)

func getChildren(arr arrow.Array) (ret []arrow.Array) {
	switch arr := arr.(type) {
	case *array.Struct:
		ret = make([]arrow.Array, arr.NumField())
		for i := 0; i < arr.NumField(); i++ {
			ret[i] = arr.Field(i)
		}
	case array.ListLike:
		ret = []arrow.Array{arr.ListValues()}
	}
	return
}

func getFields(typ arrow.DataType) []arrow.Field {
	if nested, ok := typ.(arrow.NestedType); ok {
		return nested.Fields()
	}
	return nil
}

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

func GetRefSchema(ref expr.ReferenceSegment, schema *arrow.Schema) (*arrow.Field, error) {
	return GetRefField(ref, schema.Fields())
}

func GetArray(ref expr.ReferenceSegment, arrs []arrow.Array) (arrow.Array, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	var out arrow.Array
	for ref != nil {
		if len(arrs) == 0 {
			return nil, fmt.Errorf("%w: %s", compute.ErrNoChildren, out.DataType())
		}

		switch f := ref.(type) {
		case *expr.StructFieldRef:
			if f.Field < 0 || f.Field >= int32(len(arrs)) {
				return nil, fmt.Errorf("%w. indices=%s", compute.ErrIndexRange, ref)
			}

			out = arrs[f.Field]
			arrs = getChildren(out)
		default:
			return nil, arrow.ErrNotImplemented
		}
		ref = ref.GetChild()
	}

	return out, nil
}

func GetColumn(ref expr.ReferenceSegment, batch arrow.Record) (arrow.Array, error) {
	return GetArray(ref, batch.Columns())
}

func GetReferencedValue(ref expr.ReferenceSegment, value compute.Datum, ext ExtensionIDSet) (compute.Datum, error) {
	if ref == nil {
		return nil, compute.ErrEmpty
	}

	for ref != nil {
		switch r := ref.(type) {
		case *expr.MapKeyRef:
			switch v := value.(type) {
			case *compute.ScalarDatum:
				s, ok := v.Value.(*scalar.Map)
				if !ok {
					return nil, arrow.ErrInvalid
				}

				dt, _, err := FromSubstraitType(r.MapKey.GetType(), ext)
				if err != nil {
					return nil, err
				}

				if !arrow.TypeEqual(dt, s.Type.(*arrow.MapType).KeyType()) {
					return nil, arrow.ErrInvalid
				}

				keyvalDatum, err := literalToDatum(r.MapKey, ext)
				if err != nil {
					return nil, err
				}

				var (
					keyval      = keyvalDatum.(*compute.ScalarDatum)
					m           = s.Value.(*array.Struct)
					keys        = m.Field(0)
					valueScalar scalar.Scalar
				)
				for i := 0; i < s.Value.Len(); i++ {
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
					return nil, arrow.ErrInvalid
				}

				value = &compute.ScalarDatum{Value: valueScalar}
			default:
				return nil, arrow.ErrNotImplemented
			}
		case *expr.StructFieldRef:
			switch v := value.(type) {
			case *compute.ArrayDatum:
				arr := v.MakeArray()
				defer arr.Release()
				next, err := GetArray(r, getChildren(arr))
				if err != nil {
					return nil, err
				}

				value = &compute.ArrayDatum{Value: next.Data()}
			case *compute.ScalarDatum:
				return nil, arrow.ErrNotImplemented
			case *compute.RecordDatum:
				next, err := GetColumn(r, v.Value)
				if err != nil {
					return nil, err
				}
				value = &compute.ArrayDatum{Value: next.Data()}
			default:
				return nil, arrow.ErrNotImplemented
			}
		case *expr.ListElementRef:
			switch v := value.(type) {
			case *compute.ScalarDatum:
				switch s := v.Value.(type) {
				case *scalar.List:
					sc, err := scalar.GetScalar(s.Value, int(r.Offset))
					if err != nil {
						return nil, err
					}
					value = &compute.ScalarDatum{Value: sc}
				case *scalar.LargeList:
					sc, err := scalar.GetScalar(s.Value, int(r.Offset))
					if err != nil {
						return nil, err
					}
					value = &compute.ScalarDatum{Value: sc}
				default:
					return nil, arrow.ErrNotImplemented
				}

			default:
				return nil, arrow.ErrNotImplemented
			}
		}

		ref = ref.GetChild()
	}

	return value, nil
}

var seed = maphash.MakeSeed()

func addHash(s expr.ReferenceSegment, h hash.Hash) {
	switch s := s.(type) {
	case *expr.StructFieldRef:
		h.Write(arrow.Int32Traits.CastToBytes([]int32{s.Field}))
	case *expr.MapKeyRef:
		h.Write([]byte(s.String()))
	case *expr.ListElementRef:
		h.Write(arrow.Int32Traits.CastToBytes([]int32{s.Offset}))
	}

	child := s.GetChild()
	if child != nil {
		addHash(child, h)
	}
}

func hashFieldRef(ref *expr.FieldReference) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)

	if ref.Root != expr.RootReference {
		// not implementing expression references yet, just root references
		// ensure uniqueness by just using the pointer
		return uint64(uintptr(unsafe.Pointer(ref)))
	}

	switch r := ref.Reference.(type) {
	case expr.ReferenceSegment:
		addHash(r, &h)
		return h.Sum64()
	case *expr.MaskExpression:
		// not implementing mask expression references yet, just root references
	}

	return uint64(uintptr(unsafe.Pointer(ref)))
}
