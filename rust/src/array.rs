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
use std::mem::*;
use std::rc::Rc;
use std::slice;
use std::str;
use std::string::String;

use super::bitmap::Bitmap;
use super::memory::*;

use bytes::{Bytes, BytesMut, BufMut};

struct ArrowVec {
    memory: *const u8,
    len: usize
}

//trait ArrowVecOps<T> {
//    fn get(&self, i: usize) -> T;
//}
//
//impl ArrowVecOps<i32> for ArrowVec {
//    fn get(&self, i: usize) -> i32 {
//        unimplemented!()
//    }
//}

pub enum ArrayData<'a> {
    Boolean(Vec<bool>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(&'a [i32]),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Utf8(ListData),
    Struct(Vec<Rc<Array<'a>>>)
}

pub struct Array<'a> {
    pub len: i32,
    pub null_count: i32,
    pub validity_bitmap: Option<Bitmap>,
    pub data: ArrayData<'a>
}

impl<'a> Array<'a> {

    pub fn new(len: usize, data: ArrayData<'a>) -> Self {
        Array { len: len as i32, data, validity_bitmap: None, null_count: 0 }
    }

    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

}

//TODO: use macros to generate this boilerplate code

impl<'a> From<Vec<bool>> for Array<'a> {
    fn from(v: Vec<bool>) -> Self {
        Array { len: v.len() as i32, null_count: 0, validity_bitmap: None, data: ArrayData::Boolean(v) }
    }
}

impl<'a> From<Vec<f32>> for Array<'a> {
    fn from(v: Vec<f32>) -> Self {
        Array { len: v.len() as i32, null_count: 0, validity_bitmap: None, data: ArrayData::Float32(v) }
    }
}

impl<'a> From<Vec<f64>> for Array<'a> {
    fn from(v: Vec<f64>) -> Self {
        Array { len: v.len() as i32, null_count: 0, validity_bitmap: None, data: ArrayData::Float64(v) }
    }
}

impl<'a> From<Vec<i32>> for Array<'a> {
    fn from(v: Vec<i32>) -> Self {
        let mem = allocate_aligned((v.len() * 4) as i64).unwrap();
        let slice = unsafe { slice::from_raw_parts(v.as_ptr(), v.len()) };
        Array { len: v.len() as i32, null_count: 0, validity_bitmap: None,
            data: ArrayData::Int32(slice) }
    }
}

impl<'a> From<Vec<Option<i32>>> for Array<'a> {
    fn from(v: Vec<Option<i32>>) -> Self {
        let mut null_count = 0;
        let mut validity_bitmap = Bitmap::new(v.len());
        for i in 0 .. v.len() {
            if v[i].is_none() {
                null_count+=1;
                validity_bitmap.clear(i);
            }
        }
        let values = v.iter().map(|x| x.unwrap_or(0)).collect::<Vec<i32>>();
        let mem = allocate_aligned((values.len() * 4) as i64).unwrap();
        let slice = unsafe { slice::from_raw_parts(values.as_ptr(), values.len()) };
        Array { len: values.len() as i32, null_count, validity_bitmap: Some(validity_bitmap), data: ArrayData::Int32(slice) }
    }
}

impl<'a> From<Vec<i64>> for Array<'a> {
    fn from(v: Vec<i64>) -> Self {
        Array { len: v.len() as i32, null_count: 0, validity_bitmap: None, data: ArrayData::Int64(v) }
    }
}

impl<'a> From<Vec<Option<i64>>> for Array<'a> {
    fn from(v: Vec<Option<i64>>) -> Self {
        let mut null_count = 0;
        let mut validity_bitmap = Bitmap::new(v.len());
        for i in 0 .. v.len() {
            if v[i].is_none() {
                null_count+=1;
            } else {
                validity_bitmap.set(i);
            }
        }
        let values = v.iter().map(|x| x.unwrap_or(0)).collect::<Vec<i64>>();
        Array { len: v.len() as i32, null_count, validity_bitmap: Some(validity_bitmap), data: ArrayData::Int64(values) }
    }
}

/// This method mostly just used for unit tests
impl<'a> From<Vec<&'static str>> for Array<'a> {
    fn from(v: Vec<&'static str>) -> Self {
        Array::from(v.iter().map(|s| s.to_string()).collect::<Vec<String>>())
    }
}

impl<'a> From<Vec<String>> for Array<'a> {
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
            validity_bitmap: None,
            data: ArrayData::Utf8(ListData { offsets, bytes: buf.freeze() })
        }
    }
}

impl<'a> From<Vec<Rc<Array<'a>>>> for Array<'a> {
    fn from(v: Vec<Rc<Array<'a>>>) -> Self {
        Array {
            len: v.len() as i32,
            null_count: 0,
            validity_bitmap: None,
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
    fn test_from_i32() {
        let a = Array::from(vec![5, 4, 3, 2, 1]);
        assert_eq!(5, a.len());
    }

    #[test]
    fn test_from_optional_i32() {
        let a = Array::from(vec![Some(1), None, Some(2), Some(3), None]);
        assert_eq!(5, a.len());
        // 1 == not null
        let validity_bitmap = a.validity_bitmap.unwrap();
        assert_eq!(true, validity_bitmap.is_set(0));
        assert_eq!(false, validity_bitmap.is_set(1));
        assert_eq!(true, validity_bitmap.is_set(2));
        assert_eq!(true, validity_bitmap.is_set(3));
        assert_eq!(false, validity_bitmap.is_set(4));
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



