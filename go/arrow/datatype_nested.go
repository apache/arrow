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

package arrow

import "fmt"

// ListType describes a nested type in which each array slot contains
// a variable-size sequence of values, all having the same relative type.
type ListType struct {
	elem DataType // DataType of the list's elements
}

// ListOf returns the list type with element type t.
// For example, if t represents int32, ListOf(t) represents []int32.
//
// ListOf panics if t is nil or invalid.
func ListOf(t DataType) *ListType {
	if t == nil {
		panic("arrow: nil DataType")
	}
	return &ListType{elem: t}
}

func (*ListType) ID() Type     { return LIST }
func (*ListType) Name() string { return "list" }

// Elem returns the ListType's element type.
func (t *ListType) Elem() DataType { return t.elem }

// StructType describes a nested type parameterized by an ordered sequence
// of relative types, called its fields.
type StructType struct {
	fields []Field
	index  map[string]int
	meta   KeyValueMetadata
}

// StructOf returns the struct type with fields fs.
//
// StructOf panics if there are duplicated fields.
// StructOf panics if there is a field with an invalid DataType.
func StructOf(fs ...Field) *StructType {
	n := len(fs)
	if n == 0 {
		return &StructType{}
	}

	t := &StructType{
		fields: make([]Field, n),
		index:  make(map[string]int, n),
	}
	for i, f := range fs {
		if f.Type == nil {
			panic("arrow: field with nil DataType")
		}
		t.fields[i] = Field{
			Name:     f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: f.Metadata.clone(),
		}
		if _, dup := t.index[f.Name]; dup {
			panic(fmt.Errorf("arrow: duplicate field with name %q", f.Name))
		}
		t.index[f.Name] = i
	}

	return t
}

func (*StructType) ID() Type     { return STRUCT }
func (*StructType) Name() string { return "struct" }

func (t *StructType) Fields() []Field   { return t.fields }
func (t *StructType) Field(i int) Field { return t.fields[i] }

func (t *StructType) FieldByName(name string) (Field, bool) {
	i, ok := t.index[name]
	if !ok {
		return Field{}, false
	}
	return t.fields[i], true
}

type Field struct {
	Name     string           // Field name
	Type     DataType         // The field's data type
	Nullable bool             // Fields can be nullable
	Metadata KeyValueMetadata // The field's metadata, if any
}

func (f Field) HasMetadata() bool { return len(f.Metadata.keys) != 0 }

type KeyValueMetadata struct {
	keys   []string
	values []string
}

func (kv KeyValueMetadata) clone() KeyValueMetadata {
	if len(kv.keys) == 0 {
		return KeyValueMetadata{}
	}

	o := KeyValueMetadata{
		keys:   make([]string, len(kv.keys)),
		values: make([]string, len(kv.values)),
	}
	copy(o.keys, kv.keys)
	copy(o.values, kv.values)

	return o
}

var (
	_ DataType = (*ListType)(nil)
	_ DataType = (*StructType)(nil)
)
