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

package ipc

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

func TestMessageReaderBodyInAllocator(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	const numRecords = 3
	buf := writeRecordsIntoBuffer(t, numRecords)
	r := NewMessageReader(buf, WithAllocator(mem))
	defer r.Release()

	msgs := make([]*Message, 0)
	for {
		m, err := r.Message()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		m.Retain()
		msgs = append(msgs, m)
	}
	if len(msgs) != numRecords+1 {
		t.Fatalf("expected %d messages but got %d", numRecords+1, len(msgs))
	}

	if mem.CurrentAlloc() <= 0 {
		t.Fatal("message bodies should have been allocated")
	}

	for _, m := range msgs {
		m.Release()
	}
}

func writeRecordsIntoBuffer(t *testing.T, numRecords int) *bytes.Buffer {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	s, recs := getTestRecords(mem, numRecords)
	buf := new(bytes.Buffer)
	w := NewWriter(buf, WithAllocator(mem), WithSchema(s))
	for _, rec := range recs {
		err := w.Write(rec)
		rec.Release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf
}

func getTestRecords(mem memory.Allocator, numRecords int) (*arrow.Schema, []arrow.Record) {
	meta := arrow.NewMetadata([]string{}, []string{})
	s := arrow.NewSchema([]arrow.Field{
		{Name: "test-col", Type: arrow.PrimitiveTypes.Int64},
	}, &meta)

	builder := array.NewRecordBuilder(mem, s)
	defer builder.Release()

	recs := make([]arrow.Record, numRecords)
	for i := 0; i < len(recs); i++ {
		col := builder.Field(0).(*array.Int64Builder)
		for i := 0; i < 10; i++ {
			col.Append(int64(i))
		}
		recs[i] = builder.NewRecord()
	}

	return s, recs
}
