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
use super::buffer::Buffer;
use super::list::List;

pub enum ArrayData {
    Boolean(Buffer<bool>),
    Float32(Buffer<f32>),
    Float64(Buffer<f64>),
    Int8(Buffer<i8>),
    Int16(Buffer<i16>),
    Int32(Buffer<i32>),
    Int64(Buffer<i64>),
    UInt8(Buffer<u8>),
    UInt16(Buffer<u16>),
    UInt32(Buffer<u32>),
    UInt64(Buffer<u64>),
    Utf8(List<u8>),
    Struct(Vec<Rc<Array>>),
}

macro_rules! arraydata_from_primitive {
    ($DT:ty, $AT:ident) => {
        impl From<Vec<$DT>> for ArrayData {
            fn from(v: Vec<$DT>) -> Self {
                ArrayData::$AT(Buffer::from(v))
            }
        }
        impl From<Buffer<$DT>> for ArrayData {
            fn from(v: Buffer<$DT>) -> Self {
                ArrayData::$AT(v)
            }
        }
    };
}

arraydata_from_primitive!(bool, Boolean);
arraydata_from_primitive!(f32, Float32);
arraydata_from_primitive!(f64, Float64);
arraydata_from_primitive!(i8, Int8);
arraydata_from_primitive!(i16, Int16);
arraydata_from_primitive!(i32, Int32);
arraydata_from_primitive!(i64, Int64);
arraydata_from_primitive!(u8, UInt8);
arraydata_from_primitive!(u16, UInt16);
arraydata_from_primitive!(u32, UInt32);
arraydata_from_primitive!(u64, UInt64);

pub struct Array {
    /// number of elements in the array
    len: i32,
    /// number of null elements in the array
    null_count: i32,
    /// If null_count is greater than zero then the validity_bitmap will be Some(Bitmap)
    validity_bitmap: Option<Bitmap>,
    /// The array of elements
    data: ArrayData,
}

impl Array {
    /// Create a new array where there are no null values
    pub fn new(len: usize, data: ArrayData) -> Self {
        Array {
            len: len as i32,
            data,
            validity_bitmap: None,
            null_count: 0,
        }
    }

    /// Get a reference to the array data
    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    /// number of elements in the array
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// number of null elements in the array
    pub fn null_count(&self) -> usize {
        self.null_count as usize
    }

    /// If null_count is greater than zero then the validity_bitmap will be Some(Bitmap)
    pub fn validity_bitmap(&self) -> &Option<Bitmap> {
        &self.validity_bitmap
    }
}

macro_rules! array_from_primitive {
    ($DT:ty) => {
        impl From<Vec<$DT>> for Array {
            fn from(v: Vec<$DT>) -> Self {
                Array {
                    len: v.len() as i32,
                    null_count: 0,
                    validity_bitmap: None,
                    data: ArrayData::from(v),
                }
            }
        }
        impl From<Buffer<$DT>> for Array {
            fn from(v: Buffer<$DT>) -> Self {
                Array {
                    len: v.len() as i32,
                    null_count: 0,
                    validity_bitmap: None,
                    data: ArrayData::from(v),
                }
            }
        }
    };
}

array_from_primitive!(bool);
array_from_primitive!(f32);
array_from_primitive!(f64);
array_from_primitive!(u8);
array_from_primitive!(u16);
array_from_primitive!(u32);
array_from_primitive!(u64);
array_from_primitive!(i8);
array_from_primitive!(i16);
array_from_primitive!(i32);
array_from_primitive!(i64);

macro_rules! array_from_optional_primitive {
    ($DT:ty, $DEFAULT:expr) => {
        impl From<Vec<Option<$DT>>> for Array {
            fn from(v: Vec<Option<$DT>>) -> Self {
                let mut null_count = 0;
                let mut validity_bitmap = Bitmap::new(v.len());
                for i in 0..v.len() {
                    if v[i].is_none() {
                        null_count += 1;
                        validity_bitmap.clear(i);
                    }
                }
                let values = v.iter()
                    .map(|x| x.unwrap_or($DEFAULT))
                    .collect::<Vec<$DT>>();
                Array {
                    len: values.len() as i32,
                    null_count,
                    validity_bitmap: Some(validity_bitmap),
                    data: ArrayData::from(values),
                }
            }
        }
    };
}

array_from_optional_primitive!(bool, false);
array_from_optional_primitive!(f32, 0_f32);
array_from_optional_primitive!(f64, 0_f64);
array_from_optional_primitive!(u8, 0_u8);
array_from_optional_primitive!(u16, 0_u16);
array_from_optional_primitive!(u32, 0_u32);
array_from_optional_primitive!(u64, 0_u64);
array_from_optional_primitive!(i8, 0_i8);
array_from_optional_primitive!(i16, 0_i16);
array_from_optional_primitive!(i32, 0_i32);
array_from_optional_primitive!(i64, 0_i64);

/// This method mostly just used for unit tests
impl From<Vec<&'static str>> for Array {
    fn from(v: Vec<&'static str>) -> Self {
        Array::from(v.iter().map(|s| s.to_string()).collect::<Vec<String>>())
    }
}

impl From<Vec<String>> for Array {
    fn from(v: Vec<String>) -> Self {
        Array {
            len: v.len() as i32,
            null_count: 0,
            validity_bitmap: None,
            data: ArrayData::Utf8(List::from(v)),
        }
    }
}

impl From<Vec<Rc<Array>>> for Array {
    fn from(v: Vec<Rc<Array>>) -> Self {
        Array {
            len: v.len() as i32,
            null_count: 0,
            validity_bitmap: None,
            data: ArrayData::Struct(v.iter().map(|a| a.clone()).collect()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::datatypes::*;
    use super::*;

    #[test]
    fn test_utf8_offsets() {
        let a = Array::from(vec!["this", "is", "a", "test"]);
        assert_eq!(4, a.len());
        match *a.data() {
            ArrayData::Utf8(ref list) => {
                assert_eq!(11, list.data().len());
                assert_eq!(0, *list.offsets().get(0));
                assert_eq!(4, *list.offsets().get(1));
                assert_eq!(6, *list.offsets().get(2));
                assert_eq!(7, *list.offsets().get(3));
                assert_eq!(11, *list.offsets().get(4));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_utf8_slices() {
        let a = Array::from(vec!["this", "is", "a", "test"]);
        match *a.data() {
            ArrayData::Utf8(ref d) => {
                assert_eq!(4, d.len());
                assert_eq!("this", str::from_utf8(d.slice(0)).unwrap());
                assert_eq!("is", str::from_utf8(d.slice(1)).unwrap());
                assert_eq!("a", str::from_utf8(d.slice(2)).unwrap());
                assert_eq!("test", str::from_utf8(d.slice(3)).unwrap());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_from_bool() {
        let a = Array::from(vec![false, false, true, false]);
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_from_f32() {
        let a = Array::from(vec![1.23, 2.34, 3.45, 4.56]);
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_from_i32() {
        let a = Array::from(vec![15, 14, 13, 12, 11]);
        assert_eq!(5, a.len());
        match *a.data() {
            ArrayData::Int32(ref b) => {
                assert_eq!(vec![15, 14, 13, 12, 11], b.iter().collect::<Vec<i32>>());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_from_empty_vec() {
        let v: Vec<i32> = vec![];
        let a = Array::from(v);
        assert_eq!(0, a.len());
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
        let _schema = DataType::Struct(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, false),
        ]);

        let a = Rc::new(Array::from(vec![1, 2, 3, 4, 5]));
        let b = Rc::new(Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]));
        let _ = Rc::new(Array::from(vec![a, b]));
    }

}
