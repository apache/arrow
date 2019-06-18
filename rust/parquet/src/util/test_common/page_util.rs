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

use crate::basic::Encoding;
use crate::column::page::Page;
use crate::data_type::DataType;
use crate::encodings::encoding::{get_encoder, Encoder};
use crate::encodings::levels::max_buffer_size;
use crate::encodings::levels::LevelEncoder;
use crate::schema::types::ColumnDescPtr;
use crate::util::memory::ByteBufferPtr;
use crate::util::memory::MemTracker;
use crate::util::memory::MemTrackerPtr;
use std::mem;
use std::rc::Rc;

pub trait DataPageBuilder {
    fn add_rep_levels(&mut self, max_level: i16, rep_levels: &[i16]);
    fn add_def_levels(&mut self, max_level: i16, def_levels: &[i16]);
    fn add_values<T: DataType>(&mut self, encoding: Encoding, values: &[T::T]);
    fn add_indices(&mut self, indices: ByteBufferPtr);
    fn consume(self) -> Page;
}

/// A utility struct for building data pages (v1 or v2). Callers must call:
///   - add_rep_levels()
///   - add_def_levels()
///   - add_values() for normal data page / add_indices() for dictionary data page
///   - consume()
/// in order to populate and obtain a data page.
pub struct DataPageBuilderImpl {
    desc: ColumnDescPtr,
    encoding: Option<Encoding>,
    mem_tracker: MemTrackerPtr,
    num_values: u32,
    buffer: Vec<u8>,
    rep_levels_byte_len: u32,
    def_levels_byte_len: u32,
    datapage_v2: bool,
}

impl DataPageBuilderImpl {
    // `num_values` is the number of non-null values to put in the data page.
    // `datapage_v2` flag is used to indicate if the generated data page should use V2
    // format or not.
    pub fn new(desc: ColumnDescPtr, num_values: u32, datapage_v2: bool) -> Self {
        DataPageBuilderImpl {
            desc,
            encoding: None,
            mem_tracker: Rc::new(MemTracker::new()),
            num_values,
            buffer: vec![],
            rep_levels_byte_len: 0,
            def_levels_byte_len: 0,
            datapage_v2,
        }
    }

    // Adds levels to the buffer and return number of encoded bytes
    fn add_levels(&mut self, max_level: i16, levels: &[i16]) -> u32 {
        let size = max_buffer_size(Encoding::RLE, max_level, levels.len());
        let mut level_encoder = LevelEncoder::v1(Encoding::RLE, max_level, vec![0; size]);
        level_encoder.put(levels).expect("put() should be OK");
        let encoded_levels = level_encoder.consume().expect("consume() should be OK");
        // Actual encoded bytes (without length offset)
        let encoded_bytes = &encoded_levels[mem::size_of::<i32>()..];
        if self.datapage_v2 {
            // Level encoder always initializes with offset of i32, where it stores
            // length of encoded data; for data page v2 we explicitly
            // store length, therefore we should skip i32 bytes.
            self.buffer.extend_from_slice(encoded_bytes);
        } else {
            self.buffer.extend_from_slice(encoded_levels.as_slice());
        }
        encoded_bytes.len() as u32
    }
}

impl DataPageBuilder for DataPageBuilderImpl {
    fn add_rep_levels(&mut self, max_levels: i16, rep_levels: &[i16]) {
        self.num_values = rep_levels.len() as u32;
        self.rep_levels_byte_len = self.add_levels(max_levels, rep_levels);
    }

    fn add_def_levels(&mut self, max_levels: i16, def_levels: &[i16]) {
        assert!(
            self.num_values == def_levels.len() as u32,
            "Must call `add_rep_levels() first!`"
        );

        self.def_levels_byte_len = self.add_levels(max_levels, def_levels);
    }

    fn add_values<T: DataType>(&mut self, encoding: Encoding, values: &[T::T]) {
        assert!(
            self.num_values >= values.len() as u32,
            "num_values: {}, values.len(): {}",
            self.num_values,
            values.len()
        );
        self.encoding = Some(encoding);
        let mut encoder: Box<Encoder<T>> =
            get_encoder::<T>(self.desc.clone(), encoding, self.mem_tracker.clone())
                .expect("get_encoder() should be OK");
        encoder.put(values).expect("put() should be OK");
        let encoded_values = encoder
            .flush_buffer()
            .expect("consume_buffer() should be OK");
        self.buffer.extend_from_slice(encoded_values.data());
    }

    fn add_indices(&mut self, indices: ByteBufferPtr) {
        self.encoding = Some(Encoding::RLE_DICTIONARY);
        self.buffer.extend_from_slice(indices.data());
    }

    fn consume(self) -> Page {
        if self.datapage_v2 {
            Page::DataPageV2 {
                buf: ByteBufferPtr::new(self.buffer),
                num_values: self.num_values,
                encoding: self.encoding.unwrap(),
                num_nulls: 0, /* set to dummy value - don't need this when reading
                               * data page */
                num_rows: self.num_values, /* also don't need this when reading
                                            * data page */
                def_levels_byte_len: self.def_levels_byte_len,
                rep_levels_byte_len: self.rep_levels_byte_len,
                is_compressed: false,
                statistics: None, // set to None, we do not need statistics for tests
            }
        } else {
            Page::DataPage {
                buf: ByteBufferPtr::new(self.buffer),
                num_values: self.num_values,
                encoding: self.encoding.unwrap(),
                def_level_encoding: Encoding::RLE,
                rep_level_encoding: Encoding::RLE,
                statistics: None, // set to None, we do not need statistics for tests
            }
        }
    }
}
