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
	for r.r.HasNext() {
		select {
		case <-r.readerCtx.Done():
			r.err = fmt.Errorf("avro decoding cancelled, %d records read", r.avroDatumCount)
			close(r.avroChan)
			return
		default:
			var datum any
			err := r.r.Decode(&datum)
			if err != nil {
				if errors.Is(err, io.EOF) {
					r.err = nil
					r.ocfDone <- struct{}{}
					close(r.avroChan)
					return
				}
				r.err = err
			}
			r.avroChan <- datum
			r.avroDatumCount++
		}
	}
	r.ocfDone <- struct{}{}
	close(r.avroChan)
}

func (r *OCFReader) recordFactory() {
	r.primed = true
	var drained bool
	for !r.avroDone {
		if r.chunk < 1 {
			for !drained {
				select {
				case <-r.ocfDone:
					if len(r.avroChan) == 0 {
						drained = true
						r.recChan <- r.bld.NewRecord()
						r.bldDone <- struct{}{}
						r.recDone = true
						close(r.recChan)
						return
					}
					r.avroDone = true
				default:
					if len(r.avroChan) > 0 || !r.avroDone {
						data := <-r.avroChan
						err := r.ldr.loadDatum(data)
						if err != nil {
							r.err = err
							r.done = true
							return
						}
					}
				}
			}
		} else {
			for i := 0; i < r.chunk && !drained; i++ {
				select {
				case <-r.ocfDone:
					r.avroDone = true
					if len(r.avroChan) == 0 {
						drained = true
						r.recChan <- r.bld.NewRecord()
						r.bldDone <- struct{}{}
						r.recDone = true
						close(r.recChan)
						return
					} else {
						data := <-r.avroChan
						err := r.ldr.loadDatum(data)
						if err != nil {
							r.err = err
							r.done = true
							return
						}
					}
				default:
					if len(r.avroChan) > 0 || !r.avroDone {
						data := <-r.avroChan
						err := r.ldr.loadDatum(data)
						if err != nil {
							r.err = err
							r.done = true
							return
						}
					}
				}
			}
		}
		r.recChan <- r.bld.NewRecord()
	}
	r.bldDone <- struct{}{}
	r.recDone = true
	close(r.recChan)
}
