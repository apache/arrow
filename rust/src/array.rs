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

use std::any::Any;
use std::convert::From;
use std::iter::Iterator;
use std::rc::Rc;
use std::str;
use std::string::String;

use super::bitmap::Bitmap;
use super::buffer::Buffer;
use super::datatypes::*;
use super::list::*;
use super::list_builder::*;

/// Array data type
pub trait ArrayData {
    fn len(&self) -> usize;
    fn as_any(& self) -> &Any;
}

/// Array of List<T>
pub struct ListArrayData<T: ArrowPrimitiveType> {
    len: i32,
    list: List<T>
}

impl<T> ListArrayData<T> where T: ArrowPrimitiveType {
    pub fn get(&self, i: usize) -> &[T] {
        self.list.get(i)
    }
}

impl<T> From<List<T>> for ListArrayData<T> where T: ArrowPrimitiveType {
    fn from(list: List<T>) -> Self {
        let len = list.len();
        ListArrayData { len, list }
    }
}

impl<T> ArrayData for ListArrayData<T> where T: ArrowPrimitiveType {
    fn len(&self) -> usize {
        self.len as usize
    }
    fn as_any(&self) -> &Any {
        self
    }
}

/// Array of T
pub struct BufferArrayData<T: ArrowPrimitiveType> {
    len: i32,
    data: Buffer<T>
}

impl<T> BufferArrayData<T> where T: ArrowPrimitiveType {
    pub fn len(&self) -> usize {
        self.len as usize
    }
}

impl<T> ArrayData for BufferArrayData<T> where T: ArrowPrimitiveType {
    fn len(&self) -> usize {
        self.len as usize
    }
    fn as_any(&self) -> &Any {
        self
    }
}

impl<T> From<Buffer<T>> for BufferArrayData<T> where T: ArrowPrimitiveType {
    fn from(data: Buffer<T>) -> Self {
        BufferArrayData {
            len: data.len() as i32,
            data
        }
    }
}

pub struct StructArrayData {
    len: i32,
    data: Vec<Rc<ArrayData>>
}

impl ArrayData for StructArrayData {
    fn len(&self) -> usize {
        self.len as usize
    }
    fn as_any(&self) -> &Any {
        self
    }
}

/// Top level array type, just a holder for a boxed trait for the data it contains
pub struct Array {
    len: i32,
    data: Rc<ArrayData>,
    validity_bitmap: Option<Bitmap>
}

impl Array {

    pub fn new(data: Rc<ArrayData>) -> Self {
        let len = data.len();
        Array {
            len: len as i32,
            data: data.clone(),
            validity_bitmap: None }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn data(&self) -> &Rc<ArrayData> {
        &self.data
    }
}

impl <T> From<Buffer<T>> for Array where T: ArrowPrimitiveType + 'static {
    fn from(buffer: Buffer<T>) -> Self {
        Array {
            len: buffer.len(),
            validity_bitmap: None,
            data: Rc::new(BufferArrayData::from(buffer))
        }
    }
}

impl <T> From<List<T>> for Array where T: ArrowPrimitiveType + 'static {
    fn from(list: List<T>) -> Self {
        Array {
            len: list.len() as i32,
            validity_bitmap: None,
            data: Rc::new(ListArrayData::from(list))
        }
    }
}

/// Create an Array from a Vec<T> of primitive values
impl<T> From<Vec<T>> for Array where T: ArrowPrimitiveType + 'static {
    fn from(vec: Vec<T>) -> Self {
        let len = vec.len() as i32;
        let data: Rc<ArrayData> = Rc::new(BufferArrayData::from(Buffer::from(vec)));
        Array {
            len,
            validity_bitmap: None,
            data
        }
    }
}

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
                    len: v.len() as i32,
                    validity_bitmap: Some(validity_bitmap),
                    data: Rc::new(BufferArrayData::from(Buffer::from(values))),
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

///// This method mostly just used for unit tests
impl From<Vec<&'static str>> for Array {
    fn from(v: Vec<&'static str>) -> Self {
        let mut builder: ListBuilder<u8> = ListBuilder::with_capacity(v.len());
        for s in v {
            builder.push(s.as_bytes())
        }
        Array::from(builder.finish())
    }
}

impl From<Vec<String>> for Array {
    fn from(v: Vec<String>) -> Self {
        let mut builder: ListBuilder<u8> = ListBuilder::with_capacity(v.len());
        for s in v {
            builder.push(s.as_bytes())
        }
        Array::from(builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::super::array::*;
    use super::super::datatypes::*;
    use super::super::list_builder::*;
    use super::*;

    #[test]
    fn array_data_from_list_u8() {
        let mut b: ListBuilder<u8> = ListBuilder::new();
        b.push(&[1,2,3,4,5]);
        b.push(&[5,4,3,2,1]);
        let array_data = ListArrayData::from(b.finish());
        assert_eq!(2, array_data.len());
    }

    #[test]
    fn array_from_list_u8() {
        let mut b: ListBuilder<u8> = ListBuilder::new();
        b.push("Hello, ".as_bytes());
        b.push("World!".as_bytes());
        let array = Array::from(b.finish());
        // downcast back to the data
        let array_list_u8 = array.data().as_any().downcast_ref::<ListArrayData<u8>>().unwrap();
        assert_eq!(2, array_list_u8.len());
        assert_eq!("Hello, ", str::from_utf8(array_list_u8.get(0)).unwrap());
        assert_eq!("World!", str::from_utf8(array_list_u8.get(1)).unwrap());
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

        match a.data.as_any().downcast_ref::<BufferArrayData<i32>>() {
            Some(ref buf) => {
                assert_eq!(5, buf.len())
                //TODO: assert_eq!(vec![15, 14, 13, 12, 11], buf.iter().collect::<Vec<i32>>());
            }
            _ => panic!()
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
        let a = Rc::new(BufferArrayData::from(Buffer::from(vec![1, 2, 3, 4, 5])));
        let b = Rc::new(BufferArrayData::from(Buffer::from(vec![1.1, 2.2, 3.3, 4.4, 5.5])));

        let _ = StructArrayData {
            len: 2,
            data: vec![a, b]
        };
    }

}
