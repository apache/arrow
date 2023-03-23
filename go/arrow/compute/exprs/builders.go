// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package exprs

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/extensions"
	"github.com/substrait-io/substrait-go/types"
)

func NewDefaultExtensionSet() ExtensionIDSet {
	return NewExtensionSetDefault(expr.NewEmptyExtensionRegistry(&extensions.DefaultCollection))
}

// NewScalarCall constructs a substrait ScalarFunction expression with the provided
// options and arguments.
//
// The function name (fn) is looked up in the internal Arrow DefaultExtensionIDRegistry
// to ensure it exists and to convert from the Arrow function name to the substrait
// function name. It is then looked up using the DefaultCollection from the
// substrait extensions module to find the declaration. If it cannot be found,
// we try constructing the compound signature name by getting the types of the
// arguments which were passed and appending them to the function name appropriately.
//
// An error is returned if the function cannot be resolved.
func NewScalarCall(reg ExtensionIDSet, fn string, opts []*types.FunctionOption, args ...types.FuncArg) (*expr.ScalarFunction, error) {
	conv, ok := reg.GetArrowRegistry().GetArrowToSubstrait(fn)
	if !ok {
		return nil, arrow.ErrNotFound
	}

	id, convOpts, err := conv(fn)
	if err != nil {
		return nil, err
	}

	opts = append(opts, convOpts...)
	return expr.NewScalarFunc(reg.GetSubstraitRegistry(), id, opts, args...)
}

// NewFieldRefFromDotPath constructs a substrait reference segment from
// a dot path and the base schema.
//
// dot_path = '.' name
//
//	| '[' digit+ ']'
//	| dot_path+
//
// # Examples
//
// Assume root schema of {alpha: i32, beta: struct<gamma: list<i32>>, delta: map<string, i32>}
//
//	".alpha" => StructFieldRef(0)
//	"[2]" => StructFieldRef(2)
//	".beta[0]" => StructFieldRef(1, StructFieldRef(0))
//	"[1].gamma[3]" => StructFieldRef(1, StructFieldRef(0, ListElementRef(3)))
//	".delta.foobar" => StructFieldRef(2, MapKeyRef("foobar"))
//
// Note: when parsing a name, a '\' preceding any other character
// will be dropped from the resulting name. Therefore if a name must
// contain the characters '.', '\', '[', or ']' then they must be escaped
// with a preceding '\'.
func NewFieldRefFromDotPath(dotpath string, rootSchema *arrow.Schema) (expr.ReferenceSegment, error) {
	if len(dotpath) == 0 {
		return nil, fmt.Errorf("%w dotpath was empty", arrow.ErrInvalid)
	}

	parseName := func() string {
		var name string
		for {
			idx := strings.IndexAny(dotpath, `\[.`)
			if idx == -1 {
				name += dotpath
				dotpath = ""
				break
			}

			if dotpath[idx] != '\\' {
				// subscript for a new field ref
				name += dotpath[:idx]
				dotpath = dotpath[idx:]
				break
			}

			if len(dotpath) == idx+1 {
				// dotpath ends with a backslash; consume it all
				name += dotpath
				dotpath = ""
				break
			}

			// append all characters before backslash, then the character which follows it
			name += dotpath[:idx] + string(dotpath[idx+1])
			dotpath = dotpath[idx+2:]
		}
		return name
	}

	var curType arrow.DataType = arrow.StructOf(rootSchema.Fields()...)
	children := make([]expr.ReferenceSegment, 0)

	for len(dotpath) > 0 {
		subscript := dotpath[0]
		dotpath = dotpath[1:]
		switch subscript {
		case '.':
			// next element is a name
			n := parseName()
			switch ct := curType.(type) {
			case *arrow.StructType:
				idx, found := ct.FieldIdx(n)
				if !found {
					return nil, fmt.Errorf("%w: dot path '%s' referenced invalid field", arrow.ErrInvalid, dotpath)
				}
				children = append(children, &expr.StructFieldRef{Field: int32(idx)})
				curType = ct.Field(idx).Type
			case *arrow.MapType:
				curType = ct.KeyType()
				switch ct.KeyType().ID() {
				case arrow.BINARY, arrow.LARGE_BINARY:
					children = append(children, &expr.MapKeyRef{MapKey: expr.NewByteSliceLiteral([]byte(n), false)})
				case arrow.STRING, arrow.LARGE_STRING:
					children = append(children, &expr.MapKeyRef{MapKey: expr.NewPrimitiveLiteral(n, false)})
				default:
					return nil, fmt.Errorf("%w: MapKeyRef to non-binary/string map not supported", arrow.ErrNotImplemented)
				}
			default:
				return nil, fmt.Errorf("%w: dot path names must refer to struct fields or map keys", arrow.ErrInvalid)
			}
		case '[':
			subend := strings.IndexFunc(dotpath, func(r rune) bool { return !unicode.IsDigit(r) })
			if subend == -1 || dotpath[subend] != ']' {
				return nil, fmt.Errorf("%w: dot path '%s' contained an unterminated index", arrow.ErrInvalid, dotpath)
			}
			idx, _ := strconv.Atoi(dotpath[:subend])
			switch ct := curType.(type) {
			case *arrow.StructType:
				if idx > len(ct.Fields()) {
					return nil, fmt.Errorf("%w: field out of bounds in dotpath", arrow.ErrIndex)
				}
				curType = ct.Field(idx).Type
				children = append(children, &expr.StructFieldRef{Field: int32(idx)})
			case *arrow.MapType:
				curType = ct.KeyType()
				var keyLiteral expr.Literal
				// TODO: implement user defined types and variations
				switch ct.KeyType().ID() {
				case arrow.INT8:
					keyLiteral = expr.NewPrimitiveLiteral(int8(idx), false)
				case arrow.INT16:
					keyLiteral = expr.NewPrimitiveLiteral(int16(idx), false)
				case arrow.INT32:
					keyLiteral = expr.NewPrimitiveLiteral(int32(idx), false)
				case arrow.INT64:
					keyLiteral = expr.NewPrimitiveLiteral(int64(idx), false)
				case arrow.FLOAT32:
					keyLiteral = expr.NewPrimitiveLiteral(float32(idx), false)
				case arrow.FLOAT64:
					keyLiteral = expr.NewPrimitiveLiteral(float64(idx), false)
				default:
					return nil, fmt.Errorf("%w: dotpath ref to map key type %s", arrow.ErrNotImplemented, ct.KeyType())
				}
				children = append(children, &expr.MapKeyRef{MapKey: keyLiteral})
			case *arrow.ListType:
				curType = ct.Elem()
				children = append(children, &expr.ListElementRef{Offset: int32(idx)})
			case *arrow.LargeListType:
				curType = ct.Elem()
				children = append(children, &expr.ListElementRef{Offset: int32(idx)})
			case *arrow.FixedSizeListType:
				curType = ct.Elem()
				children = append(children, &expr.ListElementRef{Offset: int32(idx)})
			default:
				return nil, fmt.Errorf("%w: %s type not supported for dotpath ref", arrow.ErrInvalid, ct)
			}
			dotpath = dotpath[subend+1:]
		default:
			return nil, fmt.Errorf("%w: dot path must begin with '[' or '.' got '%s'",
				arrow.ErrInvalid, dotpath)
		}
	}

	out := children[0]
	if len(children) > 1 {
		cur := out
		for _, c := range children[1:] {
			switch r := cur.(type) {
			case *expr.StructFieldRef:
				r.Child = c
			case *expr.MapKeyRef:
				r.Child = c
			case *expr.ListElementRef:
				r.Child = c
			}
			cur = c
		}
	}

	return out, nil
}
