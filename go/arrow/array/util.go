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

package array

import (
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v9/arrow"
	"github.com/apache/arrow/go/v9/arrow/bitutil"
	"github.com/apache/arrow/go/v9/arrow/memory"
	"github.com/apache/arrow/go/v9/internal/hashing"
	"github.com/goccy/go-json"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type fromJSONCfg struct {
	multiDocument bool
	startOffset   int64
	useNumber     bool
}

type FromJSONOption func(*fromJSONCfg)

func WithMultipleDocs() FromJSONOption {
	return func(c *fromJSONCfg) {
		c.multiDocument = true
	}
}

// WithStartOffset attempts to start decoding from the reader at the offset
// passed in. If using this option the reader must fulfill the io.ReadSeeker
// interface, or else an error will be returned.
//
// It will call Seek(off, io.SeekStart) on the reader
func WithStartOffset(off int64) FromJSONOption {
	return func(c *fromJSONCfg) {
		c.startOffset = off
	}
}

// WithUseNumber enables the 'UseNumber' option on the json decoder, using
// the json.Number type instead of assuming float64 for numbers. This is critical
// if you have numbers that are larger than what can fit into the 53 bits of
// an IEEE float64 mantissa and want to preserve its value.
func WithUseNumber() FromJSONOption {
	return func(c *fromJSONCfg) {
		c.useNumber = true
	}
}

// FromJSON creates an arrow.Array from a corresponding JSON stream and defined data type. If the types in the
// json do not match the type provided, it will return errors. This is *not* the integration test format
// and should not be used as such. This intended to be used by consumers more similarly to the current exposing of
// the csv reader/writer. It also returns the input offset in the reader where it finished decoding since buffering
// by the decoder could leave the reader's cursor past where the parsing finished if attempting to parse multiple json
// arrays from one stream.
//
// All the Array types implement json.Marshaller and thus can be written to json
// using the json.Marshal function
//
// The JSON provided must be formatted in one of two ways:
//		Default: the top level of the json must be a list which matches the type specified exactly
//		Example: `[1, 2, 3, 4, 5]` for any integer type or `[[...], null, [], .....]` for a List type
//					Struct arrays are represented a list of objects: `[{"foo": 1, "bar": "moo"}, {"foo": 5, "bar": "baz"}]`
//
//		Using WithMultipleDocs:
//			If the JSON provided is multiple newline separated json documents, then use this option
// 			and each json document will be treated as a single row of the array. This is most useful for record batches
// 			and interacting with other processes that use json. For example:
//				`{"col1": 1, "col2": "row1", "col3": ...}\n{"col1": 2, "col2": "row2", "col3": ...}\n.....`
//
// Duration values get formated upon marshalling as a string consisting of their numeric
// value followed by the unit suffix such as "10s" for a value of 10 and unit of Seconds.
// with "ms" for millisecond, "us" for microsecond, and "ns" for nanosecond as the suffixes.
// Unmarshalling duration values is more permissive since it first tries to use Go's
// time.ParseDuration function which means it allows values in the form 3h25m0.3s in addition
// to the same values which are output.
//
// Interval types are marshalled / unmarshalled as follows:
//  MonthInterval is marshalled as an object with the format:
//	 { "months": #}
//  DayTimeInterval is marshalled using Go's regular marshalling of structs:
//	 { "days": #, "milliseconds": # }
//  MonthDayNanoInterval values are marshalled the same as DayTime using Go's struct marshalling:
//   { "months": #, "days": #, "nanoseconds": # }
//
// Times use a format of HH:MM or HH:MM:SS[.zzz] where the fractions of a second cannot
// exceed the precision allowed by the time unit, otherwise unmarshalling will error.
//
// Dates use YYYY-MM-DD format
//
// Timestamps use RFC3339Nano format except without a timezone, all of the following are valid:
//	YYYY-MM-DD
//	YYYY-MM-DD[T]HH
//	YYYY-MM-DD[T]HH:MM
//  YYYY-MM-DD[T]HH:MM:SS[.zzzzzzzzzz]
//
// The fractions of a second cannot exceed the precision allowed by the timeunit of the datatype.
//
// When processing structs as objects order of keys does not matter, but keys cannot be repeated.
func FromJSON(mem memory.Allocator, dt arrow.DataType, r io.Reader, opts ...FromJSONOption) (arr arrow.Array, offset int64, err error) {
	var cfg fromJSONCfg
	for _, o := range opts {
		o(&cfg)
	}

	if cfg.startOffset != 0 {
		seeker, ok := r.(io.ReadSeeker)
		if !ok {
			return nil, 0, errors.New("using StartOffset option requires reader to be a ReadSeeker, cannot seek")
		}

		seeker.Seek(cfg.startOffset, io.SeekStart)
	}

	bldr := NewBuilder(mem, dt)
	defer bldr.Release()

	dec := json.NewDecoder(r)
	defer func() {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("failed parsing json: %w", io.ErrUnexpectedEOF)
		}
	}()

	if cfg.useNumber {
		dec.UseNumber()
	}

	if !cfg.multiDocument {
		t, err := dec.Token()
		if err != nil {
			return nil, dec.InputOffset(), err
		}

		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			return nil, dec.InputOffset(), fmt.Errorf("json doc must be an array, found %s", delim)
		}
	}

	if err = bldr.unmarshal(dec); err != nil {
		return nil, dec.InputOffset(), err
	}

	if !cfg.multiDocument {
		// consume the last ']'
		if _, err = dec.Token(); err != nil {
			return nil, dec.InputOffset(), err
		}
	}

	return bldr.NewArray(), dec.InputOffset(), nil
}

// RecordToStructArray constructs a struct array from the columns of the record batch
// by referencing them, zero-copy.
func RecordToStructArray(rec arrow.Record) *Struct {
	cols := make([]arrow.ArrayData, rec.NumCols())
	for i, c := range rec.Columns() {
		cols[i] = c.Data()
	}

	data := NewData(arrow.StructOf(rec.Schema().Fields()...), int(rec.NumRows()), []*memory.Buffer{nil}, cols, 0, 0)
	defer data.Release()

	return NewStructData(data)
}

// RecordFromStructArray is a convenience function for converting a struct array into
// a record batch without copying the data. If the passed in schema is nil, the fields
// of the struct will be used to define the record batch. Otherwise the passed in
// schema will be used to create the record batch. If passed in, the schema must match
// the fields of the struct column.
func RecordFromStructArray(in *Struct, schema *arrow.Schema) arrow.Record {
	if schema == nil {
		schema = arrow.NewSchema(in.DataType().(*arrow.StructType).Fields(), nil)
	}

	return NewRecord(schema, in.fields, int64(in.Len()))
}

// RecordFromJSON creates a record batch from JSON data. See array.FromJSON for the details
// of formatting and logic.
//
// A record batch from JSON is equivalent to reading a struct array in from json and then
// converting it to a record batch.
func RecordFromJSON(mem memory.Allocator, schema *arrow.Schema, r io.Reader, opts ...FromJSONOption) (arrow.Record, int64, error) {
	st := arrow.StructOf(schema.Fields()...)
	arr, off, err := FromJSON(mem, st, r, opts...)
	if err != nil {
		return nil, off, err
	}
	defer arr.Release()

	return RecordFromStructArray(arr.(*Struct), schema), off, nil
}

// RecordToJSON writes out the given record following the format of each row is a single object
// on a single line of the output.
func RecordToJSON(rec arrow.Record, w io.Writer) error {
	enc := json.NewEncoder(w)

	fields := rec.Schema().Fields()

	cols := make(map[string]interface{})
	for i := 0; int64(i) < rec.NumRows(); i++ {
		for j, c := range rec.Columns() {
			cols[fields[j].Name] = c.(arraymarshal).getOneForMarshal(i)
		}
		if err := enc.Encode(cols); err != nil {
			return err
		}
	}
	return nil
}

func getDictArrayData(mem memory.Allocator, valueType arrow.DataType, memoTable hashing.MemoTable, startOffset int) (*Data, error) {
	dictLen := memoTable.Size() - startOffset
	buffers := []*memory.Buffer{nil, nil}

	buffers[1] = memory.NewResizableBuffer(mem)
	defer buffers[1].Release()

	switch tbl := memoTable.(type) {
	case hashing.NumericMemoTable:
		nbytes := tbl.TypeTraits().BytesRequired(dictLen)
		buffers[1].Resize(nbytes)
		tbl.WriteOutSubset(startOffset, buffers[1].Bytes())
	case *hashing.BinaryMemoTable:
		switch valueType.ID() {
		case arrow.BINARY, arrow.STRING:
			buffers = append(buffers, memory.NewResizableBuffer(mem))
			defer buffers[2].Release()

			buffers[1].Resize(arrow.Int32Traits.BytesRequired(dictLen + 1))
			offsets := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
			tbl.CopyOffsetsSubset(startOffset, offsets)

			valuesz := offsets[len(offsets)-1] - offsets[0]
			buffers[2].Resize(int(valuesz))
			tbl.CopyValuesSubset(startOffset, buffers[2].Bytes())
		default: // fixed size
			bw := int(bitutil.BytesForBits(int64(valueType.(arrow.FixedWidthDataType).BitWidth())))
			buffers[1].Resize(dictLen * bw)
			tbl.CopyFixedWidthValues(startOffset, bw, buffers[1].Bytes())
		}
	default:
		return nil, fmt.Errorf("arrow/array: dictionary unifier unimplemented type: %s", valueType)
	}

	var nullcount int
	if idx, ok := memoTable.GetNull(); ok && idx >= startOffset {
		buffers[0] = memory.NewResizableBuffer(mem)
		defer buffers[0].Release()
		nullcount = 1
		buffers[0].Resize(int(bitutil.BytesForBits(int64(dictLen))))
		memory.Set(buffers[0].Bytes(), 0xFF)
		bitutil.ClearBit(buffers[0].Bytes(), idx)
	}

	return NewData(valueType, dictLen, buffers, nil, nullcount, 0), nil
}
