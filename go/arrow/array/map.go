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

package array // import "github.com/apache/arrow/go/arrow/array"

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/memory"
)

// Map represents an immutable sequence of Key/Value structs. It is a
// logical type that is implemented as a List<Struct: key, value>.
type Map struct {
	*List
	keys, items Interface
}

// NewMapData returns a new Map array value, from data
func NewMapData(data *Data) *Map {
	a := &Map{List: &List{}}
	a.refCount = 1
	a.setData(data)
	return a
}

// KeysSorted checks the datatype that was used to construct this array and
// returns the KeysSorted boolean value used to denote if the key array is
// sorted for each list element.
func (a *Map) KeysSorted() bool { return a.DataType().(*arrow.MapType).KeysSorted }

func (a *Map) validateData(data *Data) {
	if len(data.childData) != 1 || data.childData[0] == nil {
		panic("arrow/array: expected one child array for map array")
	}

	if data.childData[0].dtype.ID() != arrow.STRUCT {
		panic("arrow/array: map array child should be struct type")
	}

	if data.childData[0].NullN() != 0 {
		panic("arrow/array: map array child array should have no nulls")
	}

	if len(data.childData[0].childData) != 2 {
		panic("arrow/array: map array child array should have two fields")
	}

	if data.childData[0].childData[0].NullN() != 0 {
		panic("arrow/array: map array keys array should have no nulls")
	}
}

func (a *Map) setData(data *Data) {
	a.validateData(data)

	a.List.setData(data)
	a.keys = MakeFromData(data.childData[0].childData[0])
	a.items = MakeFromData(data.childData[0].childData[1])
}

// Keys returns the full Array of Key values, equivalent to grabbing
// the key field of the child struct.
func (a *Map) Keys() Interface { return a.keys }

// Items returns the full Array of Item values, equivalent to grabbing
// the Value field (the second field) of the child struct.
func (a *Map) Items() Interface { return a.items }

func (a *Map) Retain() {
	a.List.Retain()
	a.keys.Retain()
	a.items.Retain()
}

func (a *Map) Release() {
	a.List.Release()
	a.keys.Release()
	a.items.Release()
}

func arrayEqualMap(left, right *Map) bool {
	// since Map is implemented using a list, we can just use arrayEqualList
	return arrayEqualList(left.List, right.List)
}

type MapBuilder struct {
	listBuilder *ListBuilder

	etype                   arrow.DataType
	keytype, itemtype       arrow.DataType
	keyBuilder, itemBuilder Builder
	keysSorted              bool
}

// NewMapBuilder returns a builder, using the provided memory allocator.
// The created Map builder will create a map array whose keys will be a non-nullable
// array of type `keytype` and whose mapped items will be a nullable array of itemtype.
func NewMapBuilder(mem memory.Allocator, keytype, itemtype arrow.DataType, keysSorted bool) *MapBuilder {
	etype := arrow.MapOf(keytype, itemtype)
	etype.KeysSorted = keysSorted
	listBldr := NewListBuilder(mem, etype.ValueType())
	keyBldr := listBldr.ValueBuilder().(*StructBuilder).FieldBuilder(0)
	keyBldr.Retain()
	itemBldr := listBldr.ValueBuilder().(*StructBuilder).FieldBuilder(1)
	itemBldr.Retain()
	return &MapBuilder{
		listBuilder: listBldr,
		keyBuilder:  keyBldr,
		itemBuilder: itemBldr,
		etype:       etype,
		keytype:     keytype,
		itemtype:    itemtype,
		keysSorted:  keysSorted,
	}
}

func (b *MapBuilder) Retain() {
	b.listBuilder.Retain()
	b.keyBuilder.Retain()
	b.itemBuilder.Retain()
}

func (b *MapBuilder) Release() {
	b.listBuilder.Release()
	b.keyBuilder.Release()
	b.itemBuilder.Release()
}

// Len returns the current number of Maps that are in the builder
func (b *MapBuilder) Len() int { return b.listBuilder.Len() }

func (b *MapBuilder) Cap() int   { return b.listBuilder.Cap() }
func (b *MapBuilder) NullN() int { return b.listBuilder.NullN() }

// Append adds a new Map element to the array, calling Append(false) is
// equivalent to calling AppendNull.
func (b *MapBuilder) Append(v bool) {
	b.adjustStructBuilderLen()
	b.listBuilder.Append(v)
}

// AppendNull adds a null map entry to the array.
func (b *MapBuilder) AppendNull() {
	b.Append(false)
}

// Reserve enough space for n maps
func (b *MapBuilder) Reserve(n int) { b.listBuilder.Reserve(n) }

// Resize adjust the space allocated by b to n map elements. If n is greater than
// b.Cap(), additional memory will be allocated. If n is smaller, the allocated memory may be reduced.
func (b *MapBuilder) Resize(n int) { b.listBuilder.Resize(n) }

// AppendValues is for bulk appending a group of elements with offsets provided
// and validity booleans provided.
func (b *MapBuilder) AppendValues(offsets []int32, valid []bool) {
	b.adjustStructBuilderLen()
	b.listBuilder.AppendValues(offsets, valid)
}

func (b *MapBuilder) init(capacity int)                  { b.listBuilder.init(capacity) }
func (b *MapBuilder) resize(newBits int, init func(int)) { b.listBuilder.resize(newBits, init) }

func (b *MapBuilder) adjustStructBuilderLen() {
	sb := b.listBuilder.ValueBuilder().(*StructBuilder)
	if sb.Len() < b.keyBuilder.Len() {
		valids := make([]bool, b.keyBuilder.Len()-sb.Len())
		for i := range valids {
			valids[i] = true
		}
		sb.AppendValues(valids)
	}
}

// NewArray creates a new Map array from the memory buffers used by the builder, and
// resets the builder so it can be used again to build a new Map array.
func (b *MapBuilder) NewArray() Interface {
	return b.NewMapArray()
}

// NewMapArray creates a new Map array from the memory buffers used by the builder, and
// resets the builder so it can be used again to build a new Map array.
func (b *MapBuilder) NewMapArray() (a *Map) {
	data := b.newData()
	defer data.Release()
	a = NewMapData(data)
	return
}

func (b *MapBuilder) newData() (data *Data) {
	b.adjustStructBuilderLen()
	values := b.listBuilder.NewListArray()
	defer values.Release()

	data = NewData(b.etype,
		values.Len(), values.data.buffers,
		values.data.childData, values.NullN(), 0)
	return
}

// KeyBuilder returns a builder that can be used to populate the keys of the maps.
func (b *MapBuilder) KeyBuilder() Builder { return b.keyBuilder }

// ItemBuilder returns a builder that can be used to populate the values that the
// keys point to.
func (b *MapBuilder) ItemBuilder() Builder { return b.itemBuilder }

// ValueBuilder can be used instead of separately using the Key/Item builders
// to build the list as a List of Structs rather than building the keys/items
// separately.
func (b *MapBuilder) ValueBuilder() *StructBuilder {
	return b.listBuilder.ValueBuilder().(*StructBuilder)
}

var (
	_ Interface = (*Map)(nil)
	_ Builder   = (*MapBuilder)(nil)
)
