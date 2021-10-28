// Code generated by xxh3_memo_table.gen.go.tmpl. DO NOT EDIT.

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

package hashing

import (
	"math"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/bitutil"
	"github.com/apache/arrow/go/parquet/v6/internal/utils"
)

type payloadInt32 struct {
	val     int32
	memoIdx int32
}

type entryInt32 struct {
	h       uint64
	payload payloadInt32
}

func (e entryInt32) Valid() bool { return e.h != sentinel }

// Int32HashTable is a hashtable specifically for int32 that
// is utilized with the MemoTable to generalize interactions for easier
// implementation of dictionaries without losing performance.
type Int32HashTable struct {
	cap     uint64
	capMask uint64
	size    uint64

	entries []entryInt32
}

// NewInt32HashTable returns a new hash table for int32 values
// initialized with the passed in capacity or 32 whichever is larger.
func NewInt32HashTable(cap uint64) *Int32HashTable {
	initCap := uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	ret := &Int32HashTable{cap: initCap, capMask: initCap - 1, size: 0}
	ret.entries = make([]entryInt32, initCap)
	return ret
}

// Reset drops all of the values in this hash table and re-initializes it
// with the specified initial capacity as if by calling New, but without having
// to reallocate the object.
func (h *Int32HashTable) Reset(cap uint64) {
	h.cap = uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	h.capMask = h.cap - 1
	h.size = 0
	h.entries = make([]entryInt32, h.cap)
}

// CopyValues is used for copying the values out of the hash table into the
// passed in slice, in the order that they were first inserted
func (h *Int32HashTable) CopyValues(out []int32) {
	h.CopyValuesSubset(0, out)
}

// CopyValuesSubset copies a subset of the values in the hashtable out, starting
// with the value at start, in the order that they were inserted.
func (h *Int32HashTable) CopyValuesSubset(start int, out []int32) {
	h.VisitEntries(func(e *entryInt32) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			out[idx] = e.payload.val
		}
	})
}

func (h *Int32HashTable) WriteOut(out []byte) {
	h.WriteOutSubset(0, out)
}

func (h *Int32HashTable) WriteOutSubset(start int, out []byte) {
	data := arrow.Int32Traits.CastFromBytes(out)
	h.VisitEntries(func(e *entryInt32) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			data[idx] = utils.ToLEInt32(e.payload.val)
		}
	})
}

func (h *Int32HashTable) needUpsize() bool { return h.size*uint64(loadFactor) >= h.cap }

func (Int32HashTable) fixHash(v uint64) uint64 {
	if v == sentinel {
		return 42
	}
	return v
}

// Lookup retrieves the entry for a given hash value assuming it's payload value returns
// true when passed to the cmp func. Returns a pointer to the entry for the given hash value,
// and a boolean as to whether it was found. It is not safe to use the pointer if the bool is false.
func (h *Int32HashTable) Lookup(v uint64, cmp func(int32) bool) (*entryInt32, bool) {
	idx, ok := h.lookup(v, h.capMask, cmp)
	return &h.entries[idx], ok
}

func (h *Int32HashTable) lookup(v uint64, szMask uint64, cmp func(int32) bool) (uint64, bool) {
	const perturbShift uint8 = 5

	var (
		idx     uint64
		perturb uint64
		e       *entryInt32
	)

	v = h.fixHash(v)
	idx = v & szMask
	perturb = (v >> uint64(perturbShift)) + 1

	for {
		e = &h.entries[idx]
		if e.h == v && cmp(e.payload.val) {
			return idx, true
		}

		if e.h == sentinel {
			return idx, false
		}

		// perturbation logic inspired from CPython's set/dict object
		// the goal is that all 64 bits of unmasked hash value eventually
		// participate int he probing sequence, to minimize clustering
		idx = (idx + perturb) & szMask
		perturb = (perturb >> uint64(perturbShift)) + 1
	}
}

func (h *Int32HashTable) upsize(newcap uint64) error {
	newMask := newcap - 1

	oldEntries := h.entries
	h.entries = make([]entryInt32, newcap)
	for _, e := range oldEntries {
		if e.Valid() {
			idx, _ := h.lookup(e.h, newMask, func(int32) bool { return false })
			h.entries[idx] = e
		}
	}
	h.cap = newcap
	h.capMask = newMask
	return nil
}

// Insert updates the given entry with the provided hash value, payload value and memo index.
// The entry pointer must have been retrieved via lookup in order to actually insert properly.
func (h *Int32HashTable) Insert(e *entryInt32, v uint64, val int32, memoIdx int32) error {
	e.h = h.fixHash(v)
	e.payload.val = val
	e.payload.memoIdx = memoIdx
	h.size++

	if h.needUpsize() {
		h.upsize(h.cap * uint64(loadFactor) * 2)
	}
	return nil
}

// VisitEntries will call the passed in function on each *valid* entry in the hash table,
// a valid entry being one which has had a value inserted into it.
func (h *Int32HashTable) VisitEntries(visit func(*entryInt32)) {
	for _, e := range h.entries {
		if e.Valid() {
			visit(&e)
		}
	}
}

// Int32MemoTable is a wrapper over the appropriate hashtable to provide an interface
// conforming to the MemoTable interface defined in the encoding package for general interactions
// regarding dictionaries.
type Int32MemoTable struct {
	tbl     *Int32HashTable
	nullIdx int32
}

// NewInt32MemoTable returns a new memotable with num entries pre-allocated to reduce further
// allocations when inserting.
func NewInt32MemoTable(num int64) *Int32MemoTable {
	return &Int32MemoTable{tbl: NewInt32HashTable(uint64(num)), nullIdx: KeyNotFound}
}

// Reset allows this table to be re-used by dumping all the data currently in the table.
func (s *Int32MemoTable) Reset() {
	s.tbl.Reset(32)
	s.nullIdx = KeyNotFound
}

// Size returns the current number of inserted elements into the table including if a null
// has been inserted.
func (s *Int32MemoTable) Size() int {
	sz := int(s.tbl.size)
	if _, ok := s.GetNull(); ok {
		sz++
	}
	return sz
}

// GetNull returns the index of an inserted null or KeyNotFound along with a bool
// that will be true if found and false if not.
func (s *Int32MemoTable) GetNull() (int, bool) {
	return int(s.nullIdx), s.nullIdx != KeyNotFound
}

// GetOrInsertNull will return the index of the null entry or insert a null entry
// if one currently doesn't exist. The found value will be true if there was already
// a null in the table, and false if it inserted one.
func (s *Int32MemoTable) GetOrInsertNull() (idx int, found bool) {
	idx, found = s.GetNull()
	if !found {
		idx = s.Size()
		s.nullIdx = int32(idx)
	}
	return
}

// CopyValues will copy the values from the memo table out into the passed in slice
// which must be of the appropriate type.
func (s *Int32MemoTable) CopyValues(out interface{}) {
	s.CopyValuesSubset(0, out)
}

// CopyValuesSubset is like CopyValues but only copies a subset of values starting
// at the provided start index
func (s *Int32MemoTable) CopyValuesSubset(start int, out interface{}) {
	s.tbl.CopyValuesSubset(start, out.([]int32))
}

func (s *Int32MemoTable) WriteOut(out []byte) {
	s.tbl.WriteOut(out)
}

func (s *Int32MemoTable) WriteOutSubset(start int, out []byte) {
	s.tbl.WriteOutSubset(start, out)
}

// Get returns the index of the requested value in the hash table or KeyNotFound
// along with a boolean indicating if it was found or not.
func (s *Int32MemoTable) Get(val interface{}) (int, bool) {

	h := hashInt(uint64(val.(int32)), 0)
	if e, ok := s.tbl.Lookup(h, func(v int32) bool { return val.(int32) == v }); ok {
		return int(e.payload.memoIdx), ok
	}
	return KeyNotFound, false
}

// GetOrInsert will return the index of the specified value in the table, or insert the
// value into the table and return the new index. found indicates whether or not it already
// existed in the table (true) or was inserted by this call (false).
func (s *Int32MemoTable) GetOrInsert(val interface{}) (idx int, found bool, err error) {

	h := hashInt(uint64(val.(int32)), 0)
	e, ok := s.tbl.Lookup(h, func(v int32) bool {
		return val.(int32) == v
	})

	if ok {
		idx = int(e.payload.memoIdx)
		found = true
	} else {
		idx = s.Size()
		s.tbl.Insert(e, h, val.(int32), int32(idx))
	}
	return
}

type payloadInt64 struct {
	val     int64
	memoIdx int32
}

type entryInt64 struct {
	h       uint64
	payload payloadInt64
}

func (e entryInt64) Valid() bool { return e.h != sentinel }

// Int64HashTable is a hashtable specifically for int64 that
// is utilized with the MemoTable to generalize interactions for easier
// implementation of dictionaries without losing performance.
type Int64HashTable struct {
	cap     uint64
	capMask uint64
	size    uint64

	entries []entryInt64
}

// NewInt64HashTable returns a new hash table for int64 values
// initialized with the passed in capacity or 32 whichever is larger.
func NewInt64HashTable(cap uint64) *Int64HashTable {
	initCap := uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	ret := &Int64HashTable{cap: initCap, capMask: initCap - 1, size: 0}
	ret.entries = make([]entryInt64, initCap)
	return ret
}

// Reset drops all of the values in this hash table and re-initializes it
// with the specified initial capacity as if by calling New, but without having
// to reallocate the object.
func (h *Int64HashTable) Reset(cap uint64) {
	h.cap = uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	h.capMask = h.cap - 1
	h.size = 0
	h.entries = make([]entryInt64, h.cap)
}

// CopyValues is used for copying the values out of the hash table into the
// passed in slice, in the order that they were first inserted
func (h *Int64HashTable) CopyValues(out []int64) {
	h.CopyValuesSubset(0, out)
}

// CopyValuesSubset copies a subset of the values in the hashtable out, starting
// with the value at start, in the order that they were inserted.
func (h *Int64HashTable) CopyValuesSubset(start int, out []int64) {
	h.VisitEntries(func(e *entryInt64) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			out[idx] = e.payload.val
		}
	})
}

func (h *Int64HashTable) WriteOut(out []byte) {
	h.WriteOutSubset(0, out)
}

func (h *Int64HashTable) WriteOutSubset(start int, out []byte) {
	data := arrow.Int64Traits.CastFromBytes(out)
	h.VisitEntries(func(e *entryInt64) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			data[idx] = utils.ToLEInt64(e.payload.val)
		}
	})
}

func (h *Int64HashTable) needUpsize() bool { return h.size*uint64(loadFactor) >= h.cap }

func (Int64HashTable) fixHash(v uint64) uint64 {
	if v == sentinel {
		return 42
	}
	return v
}

// Lookup retrieves the entry for a given hash value assuming it's payload value returns
// true when passed to the cmp func. Returns a pointer to the entry for the given hash value,
// and a boolean as to whether it was found. It is not safe to use the pointer if the bool is false.
func (h *Int64HashTable) Lookup(v uint64, cmp func(int64) bool) (*entryInt64, bool) {
	idx, ok := h.lookup(v, h.capMask, cmp)
	return &h.entries[idx], ok
}

func (h *Int64HashTable) lookup(v uint64, szMask uint64, cmp func(int64) bool) (uint64, bool) {
	const perturbShift uint8 = 5

	var (
		idx     uint64
		perturb uint64
		e       *entryInt64
	)

	v = h.fixHash(v)
	idx = v & szMask
	perturb = (v >> uint64(perturbShift)) + 1

	for {
		e = &h.entries[idx]
		if e.h == v && cmp(e.payload.val) {
			return idx, true
		}

		if e.h == sentinel {
			return idx, false
		}

		// perturbation logic inspired from CPython's set/dict object
		// the goal is that all 64 bits of unmasked hash value eventually
		// participate int he probing sequence, to minimize clustering
		idx = (idx + perturb) & szMask
		perturb = (perturb >> uint64(perturbShift)) + 1
	}
}

func (h *Int64HashTable) upsize(newcap uint64) error {
	newMask := newcap - 1

	oldEntries := h.entries
	h.entries = make([]entryInt64, newcap)
	for _, e := range oldEntries {
		if e.Valid() {
			idx, _ := h.lookup(e.h, newMask, func(int64) bool { return false })
			h.entries[idx] = e
		}
	}
	h.cap = newcap
	h.capMask = newMask
	return nil
}

// Insert updates the given entry with the provided hash value, payload value and memo index.
// The entry pointer must have been retrieved via lookup in order to actually insert properly.
func (h *Int64HashTable) Insert(e *entryInt64, v uint64, val int64, memoIdx int32) error {
	e.h = h.fixHash(v)
	e.payload.val = val
	e.payload.memoIdx = memoIdx
	h.size++

	if h.needUpsize() {
		h.upsize(h.cap * uint64(loadFactor) * 2)
	}
	return nil
}

// VisitEntries will call the passed in function on each *valid* entry in the hash table,
// a valid entry being one which has had a value inserted into it.
func (h *Int64HashTable) VisitEntries(visit func(*entryInt64)) {
	for _, e := range h.entries {
		if e.Valid() {
			visit(&e)
		}
	}
}

// Int64MemoTable is a wrapper over the appropriate hashtable to provide an interface
// conforming to the MemoTable interface defined in the encoding package for general interactions
// regarding dictionaries.
type Int64MemoTable struct {
	tbl     *Int64HashTable
	nullIdx int32
}

// NewInt64MemoTable returns a new memotable with num entries pre-allocated to reduce further
// allocations when inserting.
func NewInt64MemoTable(num int64) *Int64MemoTable {
	return &Int64MemoTable{tbl: NewInt64HashTable(uint64(num)), nullIdx: KeyNotFound}
}

// Reset allows this table to be re-used by dumping all the data currently in the table.
func (s *Int64MemoTable) Reset() {
	s.tbl.Reset(32)
	s.nullIdx = KeyNotFound
}

// Size returns the current number of inserted elements into the table including if a null
// has been inserted.
func (s *Int64MemoTable) Size() int {
	sz := int(s.tbl.size)
	if _, ok := s.GetNull(); ok {
		sz++
	}
	return sz
}

// GetNull returns the index of an inserted null or KeyNotFound along with a bool
// that will be true if found and false if not.
func (s *Int64MemoTable) GetNull() (int, bool) {
	return int(s.nullIdx), s.nullIdx != KeyNotFound
}

// GetOrInsertNull will return the index of the null entry or insert a null entry
// if one currently doesn't exist. The found value will be true if there was already
// a null in the table, and false if it inserted one.
func (s *Int64MemoTable) GetOrInsertNull() (idx int, found bool) {
	idx, found = s.GetNull()
	if !found {
		idx = s.Size()
		s.nullIdx = int32(idx)
	}
	return
}

// CopyValues will copy the values from the memo table out into the passed in slice
// which must be of the appropriate type.
func (s *Int64MemoTable) CopyValues(out interface{}) {
	s.CopyValuesSubset(0, out)
}

// CopyValuesSubset is like CopyValues but only copies a subset of values starting
// at the provided start index
func (s *Int64MemoTable) CopyValuesSubset(start int, out interface{}) {
	s.tbl.CopyValuesSubset(start, out.([]int64))
}

func (s *Int64MemoTable) WriteOut(out []byte) {
	s.tbl.WriteOut(out)
}

func (s *Int64MemoTable) WriteOutSubset(start int, out []byte) {
	s.tbl.WriteOutSubset(start, out)
}

// Get returns the index of the requested value in the hash table or KeyNotFound
// along with a boolean indicating if it was found or not.
func (s *Int64MemoTable) Get(val interface{}) (int, bool) {

	h := hashInt(uint64(val.(int64)), 0)
	if e, ok := s.tbl.Lookup(h, func(v int64) bool { return val.(int64) == v }); ok {
		return int(e.payload.memoIdx), ok
	}
	return KeyNotFound, false
}

// GetOrInsert will return the index of the specified value in the table, or insert the
// value into the table and return the new index. found indicates whether or not it already
// existed in the table (true) or was inserted by this call (false).
func (s *Int64MemoTable) GetOrInsert(val interface{}) (idx int, found bool, err error) {

	h := hashInt(uint64(val.(int64)), 0)
	e, ok := s.tbl.Lookup(h, func(v int64) bool {
		return val.(int64) == v
	})

	if ok {
		idx = int(e.payload.memoIdx)
		found = true
	} else {
		idx = s.Size()
		s.tbl.Insert(e, h, val.(int64), int32(idx))
	}
	return
}

type payloadFloat32 struct {
	val     float32
	memoIdx int32
}

type entryFloat32 struct {
	h       uint64
	payload payloadFloat32
}

func (e entryFloat32) Valid() bool { return e.h != sentinel }

// Float32HashTable is a hashtable specifically for float32 that
// is utilized with the MemoTable to generalize interactions for easier
// implementation of dictionaries without losing performance.
type Float32HashTable struct {
	cap     uint64
	capMask uint64
	size    uint64

	entries []entryFloat32
}

// NewFloat32HashTable returns a new hash table for float32 values
// initialized with the passed in capacity or 32 whichever is larger.
func NewFloat32HashTable(cap uint64) *Float32HashTable {
	initCap := uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	ret := &Float32HashTable{cap: initCap, capMask: initCap - 1, size: 0}
	ret.entries = make([]entryFloat32, initCap)
	return ret
}

// Reset drops all of the values in this hash table and re-initializes it
// with the specified initial capacity as if by calling New, but without having
// to reallocate the object.
func (h *Float32HashTable) Reset(cap uint64) {
	h.cap = uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	h.capMask = h.cap - 1
	h.size = 0
	h.entries = make([]entryFloat32, h.cap)
}

// CopyValues is used for copying the values out of the hash table into the
// passed in slice, in the order that they were first inserted
func (h *Float32HashTable) CopyValues(out []float32) {
	h.CopyValuesSubset(0, out)
}

// CopyValuesSubset copies a subset of the values in the hashtable out, starting
// with the value at start, in the order that they were inserted.
func (h *Float32HashTable) CopyValuesSubset(start int, out []float32) {
	h.VisitEntries(func(e *entryFloat32) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			out[idx] = e.payload.val
		}
	})
}

func (h *Float32HashTable) WriteOut(out []byte) {
	h.WriteOutSubset(0, out)
}

func (h *Float32HashTable) WriteOutSubset(start int, out []byte) {
	data := arrow.Float32Traits.CastFromBytes(out)
	h.VisitEntries(func(e *entryFloat32) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			data[idx] = utils.ToLEFloat32(e.payload.val)
		}
	})
}

func (h *Float32HashTable) needUpsize() bool { return h.size*uint64(loadFactor) >= h.cap }

func (Float32HashTable) fixHash(v uint64) uint64 {
	if v == sentinel {
		return 42
	}
	return v
}

// Lookup retrieves the entry for a given hash value assuming it's payload value returns
// true when passed to the cmp func. Returns a pointer to the entry for the given hash value,
// and a boolean as to whether it was found. It is not safe to use the pointer if the bool is false.
func (h *Float32HashTable) Lookup(v uint64, cmp func(float32) bool) (*entryFloat32, bool) {
	idx, ok := h.lookup(v, h.capMask, cmp)
	return &h.entries[idx], ok
}

func (h *Float32HashTable) lookup(v uint64, szMask uint64, cmp func(float32) bool) (uint64, bool) {
	const perturbShift uint8 = 5

	var (
		idx     uint64
		perturb uint64
		e       *entryFloat32
	)

	v = h.fixHash(v)
	idx = v & szMask
	perturb = (v >> uint64(perturbShift)) + 1

	for {
		e = &h.entries[idx]
		if e.h == v && cmp(e.payload.val) {
			return idx, true
		}

		if e.h == sentinel {
			return idx, false
		}

		// perturbation logic inspired from CPython's set/dict object
		// the goal is that all 64 bits of unmasked hash value eventually
		// participate int he probing sequence, to minimize clustering
		idx = (idx + perturb) & szMask
		perturb = (perturb >> uint64(perturbShift)) + 1
	}
}

func (h *Float32HashTable) upsize(newcap uint64) error {
	newMask := newcap - 1

	oldEntries := h.entries
	h.entries = make([]entryFloat32, newcap)
	for _, e := range oldEntries {
		if e.Valid() {
			idx, _ := h.lookup(e.h, newMask, func(float32) bool { return false })
			h.entries[idx] = e
		}
	}
	h.cap = newcap
	h.capMask = newMask
	return nil
}

// Insert updates the given entry with the provided hash value, payload value and memo index.
// The entry pointer must have been retrieved via lookup in order to actually insert properly.
func (h *Float32HashTable) Insert(e *entryFloat32, v uint64, val float32, memoIdx int32) error {
	e.h = h.fixHash(v)
	e.payload.val = val
	e.payload.memoIdx = memoIdx
	h.size++

	if h.needUpsize() {
		h.upsize(h.cap * uint64(loadFactor) * 2)
	}
	return nil
}

// VisitEntries will call the passed in function on each *valid* entry in the hash table,
// a valid entry being one which has had a value inserted into it.
func (h *Float32HashTable) VisitEntries(visit func(*entryFloat32)) {
	for _, e := range h.entries {
		if e.Valid() {
			visit(&e)
		}
	}
}

// Float32MemoTable is a wrapper over the appropriate hashtable to provide an interface
// conforming to the MemoTable interface defined in the encoding package for general interactions
// regarding dictionaries.
type Float32MemoTable struct {
	tbl     *Float32HashTable
	nullIdx int32
}

// NewFloat32MemoTable returns a new memotable with num entries pre-allocated to reduce further
// allocations when inserting.
func NewFloat32MemoTable(num int64) *Float32MemoTable {
	return &Float32MemoTable{tbl: NewFloat32HashTable(uint64(num)), nullIdx: KeyNotFound}
}

// Reset allows this table to be re-used by dumping all the data currently in the table.
func (s *Float32MemoTable) Reset() {
	s.tbl.Reset(32)
	s.nullIdx = KeyNotFound
}

// Size returns the current number of inserted elements into the table including if a null
// has been inserted.
func (s *Float32MemoTable) Size() int {
	sz := int(s.tbl.size)
	if _, ok := s.GetNull(); ok {
		sz++
	}
	return sz
}

// GetNull returns the index of an inserted null or KeyNotFound along with a bool
// that will be true if found and false if not.
func (s *Float32MemoTable) GetNull() (int, bool) {
	return int(s.nullIdx), s.nullIdx != KeyNotFound
}

// GetOrInsertNull will return the index of the null entry or insert a null entry
// if one currently doesn't exist. The found value will be true if there was already
// a null in the table, and false if it inserted one.
func (s *Float32MemoTable) GetOrInsertNull() (idx int, found bool) {
	idx, found = s.GetNull()
	if !found {
		idx = s.Size()
		s.nullIdx = int32(idx)
	}
	return
}

// CopyValues will copy the values from the memo table out into the passed in slice
// which must be of the appropriate type.
func (s *Float32MemoTable) CopyValues(out interface{}) {
	s.CopyValuesSubset(0, out)
}

// CopyValuesSubset is like CopyValues but only copies a subset of values starting
// at the provided start index
func (s *Float32MemoTable) CopyValuesSubset(start int, out interface{}) {
	s.tbl.CopyValuesSubset(start, out.([]float32))
}

func (s *Float32MemoTable) WriteOut(out []byte) {
	s.tbl.WriteOut(out)
}

func (s *Float32MemoTable) WriteOutSubset(start int, out []byte) {
	s.tbl.WriteOutSubset(start, out)
}

// Get returns the index of the requested value in the hash table or KeyNotFound
// along with a boolean indicating if it was found or not.
func (s *Float32MemoTable) Get(val interface{}) (int, bool) {
	var cmp func(float32) bool

	if math.IsNaN(float64(val.(float32))) {
		cmp = isNan32Cmp
		// use consistent internal bit pattern for NaN regardless of the pattern
		// that is passed to us. NaN is NaN is NaN
		val = float32(math.NaN())
	} else {
		cmp = func(v float32) bool { return val.(float32) == v }
	}

	h := hashFloat32(val.(float32), 0)
	if e, ok := s.tbl.Lookup(h, cmp); ok {
		return int(e.payload.memoIdx), ok
	}
	return KeyNotFound, false
}

// GetOrInsert will return the index of the specified value in the table, or insert the
// value into the table and return the new index. found indicates whether or not it already
// existed in the table (true) or was inserted by this call (false).
func (s *Float32MemoTable) GetOrInsert(val interface{}) (idx int, found bool, err error) {

	var cmp func(float32) bool

	if math.IsNaN(float64(val.(float32))) {
		cmp = isNan32Cmp
		// use consistent internal bit pattern for NaN regardless of the pattern
		// that is passed to us. NaN is NaN is NaN
		val = float32(math.NaN())
	} else {
		cmp = func(v float32) bool { return val.(float32) == v }
	}

	h := hashFloat32(val.(float32), 0)
	e, ok := s.tbl.Lookup(h, cmp)

	if ok {
		idx = int(e.payload.memoIdx)
		found = true
	} else {
		idx = s.Size()
		s.tbl.Insert(e, h, val.(float32), int32(idx))
	}
	return
}

type payloadFloat64 struct {
	val     float64
	memoIdx int32
}

type entryFloat64 struct {
	h       uint64
	payload payloadFloat64
}

func (e entryFloat64) Valid() bool { return e.h != sentinel }

// Float64HashTable is a hashtable specifically for float64 that
// is utilized with the MemoTable to generalize interactions for easier
// implementation of dictionaries without losing performance.
type Float64HashTable struct {
	cap     uint64
	capMask uint64
	size    uint64

	entries []entryFloat64
}

// NewFloat64HashTable returns a new hash table for float64 values
// initialized with the passed in capacity or 32 whichever is larger.
func NewFloat64HashTable(cap uint64) *Float64HashTable {
	initCap := uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	ret := &Float64HashTable{cap: initCap, capMask: initCap - 1, size: 0}
	ret.entries = make([]entryFloat64, initCap)
	return ret
}

// Reset drops all of the values in this hash table and re-initializes it
// with the specified initial capacity as if by calling New, but without having
// to reallocate the object.
func (h *Float64HashTable) Reset(cap uint64) {
	h.cap = uint64(bitutil.NextPowerOf2(int(max(cap, 32))))
	h.capMask = h.cap - 1
	h.size = 0
	h.entries = make([]entryFloat64, h.cap)
}

// CopyValues is used for copying the values out of the hash table into the
// passed in slice, in the order that they were first inserted
func (h *Float64HashTable) CopyValues(out []float64) {
	h.CopyValuesSubset(0, out)
}

// CopyValuesSubset copies a subset of the values in the hashtable out, starting
// with the value at start, in the order that they were inserted.
func (h *Float64HashTable) CopyValuesSubset(start int, out []float64) {
	h.VisitEntries(func(e *entryFloat64) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			out[idx] = e.payload.val
		}
	})
}

func (h *Float64HashTable) WriteOut(out []byte) {
	h.WriteOutSubset(0, out)
}

func (h *Float64HashTable) WriteOutSubset(start int, out []byte) {
	data := arrow.Float64Traits.CastFromBytes(out)
	h.VisitEntries(func(e *entryFloat64) {
		idx := e.payload.memoIdx - int32(start)
		if idx >= 0 {
			data[idx] = utils.ToLEFloat64(e.payload.val)
		}
	})
}

func (h *Float64HashTable) needUpsize() bool { return h.size*uint64(loadFactor) >= h.cap }

func (Float64HashTable) fixHash(v uint64) uint64 {
	if v == sentinel {
		return 42
	}
	return v
}

// Lookup retrieves the entry for a given hash value assuming it's payload value returns
// true when passed to the cmp func. Returns a pointer to the entry for the given hash value,
// and a boolean as to whether it was found. It is not safe to use the pointer if the bool is false.
func (h *Float64HashTable) Lookup(v uint64, cmp func(float64) bool) (*entryFloat64, bool) {
	idx, ok := h.lookup(v, h.capMask, cmp)
	return &h.entries[idx], ok
}

func (h *Float64HashTable) lookup(v uint64, szMask uint64, cmp func(float64) bool) (uint64, bool) {
	const perturbShift uint8 = 5

	var (
		idx     uint64
		perturb uint64
		e       *entryFloat64
	)

	v = h.fixHash(v)
	idx = v & szMask
	perturb = (v >> uint64(perturbShift)) + 1

	for {
		e = &h.entries[idx]
		if e.h == v && cmp(e.payload.val) {
			return idx, true
		}

		if e.h == sentinel {
			return idx, false
		}

		// perturbation logic inspired from CPython's set/dict object
		// the goal is that all 64 bits of unmasked hash value eventually
		// participate int he probing sequence, to minimize clustering
		idx = (idx + perturb) & szMask
		perturb = (perturb >> uint64(perturbShift)) + 1
	}
}

func (h *Float64HashTable) upsize(newcap uint64) error {
	newMask := newcap - 1

	oldEntries := h.entries
	h.entries = make([]entryFloat64, newcap)
	for _, e := range oldEntries {
		if e.Valid() {
			idx, _ := h.lookup(e.h, newMask, func(float64) bool { return false })
			h.entries[idx] = e
		}
	}
	h.cap = newcap
	h.capMask = newMask
	return nil
}

// Insert updates the given entry with the provided hash value, payload value and memo index.
// The entry pointer must have been retrieved via lookup in order to actually insert properly.
func (h *Float64HashTable) Insert(e *entryFloat64, v uint64, val float64, memoIdx int32) error {
	e.h = h.fixHash(v)
	e.payload.val = val
	e.payload.memoIdx = memoIdx
	h.size++

	if h.needUpsize() {
		h.upsize(h.cap * uint64(loadFactor) * 2)
	}
	return nil
}

// VisitEntries will call the passed in function on each *valid* entry in the hash table,
// a valid entry being one which has had a value inserted into it.
func (h *Float64HashTable) VisitEntries(visit func(*entryFloat64)) {
	for _, e := range h.entries {
		if e.Valid() {
			visit(&e)
		}
	}
}

// Float64MemoTable is a wrapper over the appropriate hashtable to provide an interface
// conforming to the MemoTable interface defined in the encoding package for general interactions
// regarding dictionaries.
type Float64MemoTable struct {
	tbl     *Float64HashTable
	nullIdx int32
}

// NewFloat64MemoTable returns a new memotable with num entries pre-allocated to reduce further
// allocations when inserting.
func NewFloat64MemoTable(num int64) *Float64MemoTable {
	return &Float64MemoTable{tbl: NewFloat64HashTable(uint64(num)), nullIdx: KeyNotFound}
}

// Reset allows this table to be re-used by dumping all the data currently in the table.
func (s *Float64MemoTable) Reset() {
	s.tbl.Reset(32)
	s.nullIdx = KeyNotFound
}

// Size returns the current number of inserted elements into the table including if a null
// has been inserted.
func (s *Float64MemoTable) Size() int {
	sz := int(s.tbl.size)
	if _, ok := s.GetNull(); ok {
		sz++
	}
	return sz
}

// GetNull returns the index of an inserted null or KeyNotFound along with a bool
// that will be true if found and false if not.
func (s *Float64MemoTable) GetNull() (int, bool) {
	return int(s.nullIdx), s.nullIdx != KeyNotFound
}

// GetOrInsertNull will return the index of the null entry or insert a null entry
// if one currently doesn't exist. The found value will be true if there was already
// a null in the table, and false if it inserted one.
func (s *Float64MemoTable) GetOrInsertNull() (idx int, found bool) {
	idx, found = s.GetNull()
	if !found {
		idx = s.Size()
		s.nullIdx = int32(idx)
	}
	return
}

// CopyValues will copy the values from the memo table out into the passed in slice
// which must be of the appropriate type.
func (s *Float64MemoTable) CopyValues(out interface{}) {
	s.CopyValuesSubset(0, out)
}

// CopyValuesSubset is like CopyValues but only copies a subset of values starting
// at the provided start index
func (s *Float64MemoTable) CopyValuesSubset(start int, out interface{}) {
	s.tbl.CopyValuesSubset(start, out.([]float64))
}

func (s *Float64MemoTable) WriteOut(out []byte) {
	s.tbl.WriteOut(out)
}

func (s *Float64MemoTable) WriteOutSubset(start int, out []byte) {
	s.tbl.WriteOutSubset(start, out)
}

// Get returns the index of the requested value in the hash table or KeyNotFound
// along with a boolean indicating if it was found or not.
func (s *Float64MemoTable) Get(val interface{}) (int, bool) {
	var cmp func(float64) bool
	if math.IsNaN(val.(float64)) {
		cmp = math.IsNaN
		// use consistent internal bit pattern for NaN regardless of the pattern
		// that is passed to us. NaN is NaN is NaN
		val = math.NaN()
	} else {
		cmp = func(v float64) bool { return val.(float64) == v }
	}

	h := hashFloat64(val.(float64), 0)
	if e, ok := s.tbl.Lookup(h, cmp); ok {
		return int(e.payload.memoIdx), ok
	}
	return KeyNotFound, false
}

// GetOrInsert will return the index of the specified value in the table, or insert the
// value into the table and return the new index. found indicates whether or not it already
// existed in the table (true) or was inserted by this call (false).
func (s *Float64MemoTable) GetOrInsert(val interface{}) (idx int, found bool, err error) {

	var cmp func(float64) bool
	if math.IsNaN(val.(float64)) {
		cmp = math.IsNaN
		// use consistent internal bit pattern for NaN regardless of the pattern
		// that is passed to us. NaN is NaN is NaN
		val = math.NaN()
	} else {
		cmp = func(v float64) bool { return val.(float64) == v }
	}

	h := hashFloat64(val.(float64), 0)
	e, ok := s.tbl.Lookup(h, cmp)

	if ok {
		idx = int(e.payload.memoIdx)
		found = true
	} else {
		idx = s.Size()
		s.tbl.Insert(e, h, val.(float64), int32(idx))
	}
	return
}
