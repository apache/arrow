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

use std::convert::From;
use std::iter::Iterator;
use std::rc::Rc;
use std::str;
use std::string::String;

use super::bitmap::Bitmap;

use bytes::{Bytes, BytesMut, BufMut};

//TODO: make arrays immutable
//TODO: make memory regions 8-byte aligned (look into RawVec and allocator API)

pub enum ArrayData {
    Boolean(Vec<bool>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Utf8(ListData),
    Struct(Vec<Rc<Array>>)
}

pub struct Array {
    pub len: i32,
    pub null_count: i32,
    pub null_bitmap: Bitmap,
    pub data: ArrayData
}

impl Array {

    pub fn new(len: usize, data: ArrayData) -> Self {
        Array { len: len as i32, data, null_bitmap: Bitmap::new(len), null_count: 0 }
    }

    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

}

//TODO: use macros to generate this boilerplate code

impl From<Vec<bool>> for Array {
    fn from(v: Vec<bool>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Boolean(v) }
    }
}

impl From<Vec<f32>> for Array {
    fn from(v: Vec<f32>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Float32(v) }
    }
}

impl From<Vec<f64>> for Array {
    fn from(v: Vec<f64>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Float64(v) }
    }
}

impl From<Vec<i32>> for Array {
    fn from(v: Vec<i32>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Int32(v) }
    }
}

impl From<Vec<Option<i32>>> for Array {
    fn from(v: Vec<Option<i32>>) -> Self {
        let mut null_count = 0;
        let mut null_bitmap = Bitmap::new(v.len());
        for i in 0 .. v.len() {
            if v[i].is_none() {
                null_count+=1;
                null_bitmap.set(i);
            }
        }
        let values = v.iter().map(|x| x.unwrap_or(0)).collect::<Vec<i32>>();
        Array { len: v.len() as i32, null_count, null_bitmap, data: ArrayData::Int32(values) }
    }
}

impl From<Vec<i64>> for Array {
    fn from(v: Vec<i64>) -> Self {
        Array { len: v.len() as i32, null_count: 0, null_bitmap: Bitmap::new(v.len()), data: ArrayData::Int64(v) }
    }
}

impl From<Vec<Option<i64>>> for Array {
    fn from(v: Vec<Option<i64>>) -> Self {
        let mut null_count = 0;
        let mut null_bitmap = Bitmap::new(v.len());
        for i in 0 .. v.len() {
            if v[i].is_none() {
                null_count+=1;
                null_bitmap.set(i);
            }
        }
        let values = v.iter().map(|x| x.unwrap_or(0)).collect::<Vec<i64>>();
        Array { len: v.len() as i32, null_count, null_bitmap, data: ArrayData::Int64(values) }
    }
}

/// This method mostly just used for unit tests
impl From<Vec<&'static str>> for Array {
    fn from(v: Vec<&'static str>) -> Self {
        Array::from(v.iter().map(|s| s.to_string()).collect::<Vec<String>>())
    }
}

impl From<Vec<String>> for Array {
    fn from(v: Vec<String>) -> Self {
        let mut offsets : Vec<i32> = Vec::with_capacity(v.len() + 1);
        let mut buf = BytesMut::with_capacity(v.len() * 32);
        offsets.push(0_i32);
        v.iter().for_each(|s| {
            buf.put(s.as_bytes());
            offsets.push(buf.len() as i32);
        });
        Array {
            len: v.len() as i32,
            null_count: 0,
            null_bitmap: Bitmap::new(v.len()),
            data: ArrayData::Utf8(ListData { offsets, bytes: buf.freeze() })
        }
    }
}

impl From<Vec<Rc<Array>>> for Array {
    fn from(v: Vec<Rc<Array>>) -> Self {
        Array {
            len: v.len() as i32,
            null_count: 0,
            null_bitmap: Bitmap::new(v.len()),
            data: ArrayData::Struct(v.iter().map(|a| a.clone()).collect())
        }
    }
}

/// List of variable-width data such as Utf8 strings
pub struct ListData {
    pub offsets: Vec<i32>,
    pub bytes: Bytes
}

impl ListData {

    pub fn len(&self) -> usize {
        self.offsets.len()-1
    }

    pub fn slice(&self, index: usize) -> &[u8] {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index+1] as usize;
        &self.bytes[start..end]
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::super::datatypes::*;

    #[test]
    fn test_utf8_offsets() {
        let a = Array::from(vec!["this", "is", "a", "test"]);
        assert_eq!(4, a.len());
        match a.data() {
            &ArrayData::Utf8(ListData { ref offsets, ref bytes }) => {
                assert_eq!(11, bytes.len());
                assert_eq!(0, offsets[0]);
                assert_eq!(4, offsets[1]);
                assert_eq!(6, offsets[2]);
                assert_eq!(7, offsets[3]);
                assert_eq!(11, offsets[4]);
            },
            _ => panic!()
        }
    }

    #[test]
    fn test_utf8_slices() {
        let a = Array::from(vec!["this", "is", "a", "test"]);
        match a.data() {
            &ArrayData::Utf8(ref d) => {
                assert_eq!(4, d.len());
                assert_eq!("this", str::from_utf8(d.slice(0)).unwrap());
                assert_eq!("is", str::from_utf8(d.slice(1)).unwrap());
                assert_eq!("a", str::from_utf8(d.slice(2)).unwrap());
                assert_eq!("test", str::from_utf8(d.slice(3)).unwrap());
            },
            _ => panic!()
        }
    }

    #[test]
    fn test_optional_i32() {
       let a = Array::from(vec![Some(1), None, Some(2), Some(3), None]);
        assert_eq!(5, a.len());
        assert_eq!(false, a.null_bitmap.is_set(0));
        assert_eq!(true, a.null_bitmap.is_set(1));
        assert_eq!(false, a.null_bitmap.is_set(2));
        assert_eq!(false, a.null_bitmap.is_set(3));
        assert_eq!(true, a.null_bitmap.is_set(4));
    }

    #[test]
    fn test_struct() {

        let _schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, false),
        ]);

        let a = Rc::new(Array::from(vec![1,2,3,4,5]));
        let b = Rc::new(Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]));
        let _ = Rc::new(Array::from(vec![a,b]));
    }
}



