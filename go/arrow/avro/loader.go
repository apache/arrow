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

package avro

import (
	"errors"
	"fmt"
	"io"
)

func (r *OCFReader) decodeOCFToChan() {
	defer close(r.avroChan)
	for r.r.HasNext() {
		select {
		case <-r.readerCtx.Done():
			r.err = fmt.Errorf("avro decoding cancelled, %d records read", r.avroDatumCount)
			return
		default:
			var datum any
			err := r.r.Decode(&datum)
			if err != nil {
				if errors.Is(err, io.EOF) {
					r.err = nil
					return
				}
				r.err = err
				return
			}
			r.avroChan <- datum
			r.avroDatumCount++
		}
	}
}

func (r *OCFReader) recordFactory() {
	defer close(r.recChan)
	r.primed = true
	recChunk := 0
	switch {
	case r.chunk < 1:
		for data := range r.avroChan {
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.err = err
				return
			}
		}
		r.recChan <- r.bld.NewRecord()
		r.bldDone <- struct{}{}
	case r.chunk >= 1:
		for data := range r.avroChan {
			if recChunk == 0 {
				r.bld.Reserve(r.chunk)
			}
			err := r.ldr.loadDatum(data)
			if err != nil {
				r.err = err
				return
			}
			recChunk++
			if recChunk >= r.chunk {
				r.recChan <- r.bld.NewRecord()
				recChunk = 0
			}
		}
		if recChunk != 0 {
			r.recChan <- r.bld.NewRecord()
		}
		r.bldDone <- struct{}{}
	}
}
