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
use std::fmt;
use std::fmt::{Error, Formatter};
use std::iter::Iterator;
use std::rc::Rc;
use std::str;
use std::string::String;

use super::bitmap::Bitmap;
use super::buffer::Buffer;
use super::datatypes::*;
use super::list::List;

struct ListArrayData<T: ArrowPrimitiveType> {
    len: i32,
    //bitmap: Bitmap,
    data: List<T>
}

impl<T> ArrowType for ListArrayData<T> where T: ArrowPrimitiveType {
    fn get_type(&self) -> DataType {
        unimplemented!()
    }
}

impl<T> ArrayData for ListArrayData<T> where T: ArrowPrimitiveType {
    fn len(&self) -> usize {
        self.len as usize
    }
}

impl<T> From<List<T>> for ListArrayData<T> where T: ArrowPrimitiveType {
    fn from(data: List<T>) -> Self {
        ListArrayData {
            len: data.len() as i32,
            data
        }
    }
}

struct BufferArrayData<T: ArrowPrimitiveType> {
    len: i32,
    //bitmap: Bitmap,
    data: Buffer<T>
}

impl<T> ArrowType for BufferArrayData<T> where T: ArrowPrimitiveType {
    fn get_type(&self) -> DataType {
        unimplemented!()
    }
}
impl<T> ArrayData for BufferArrayData<T> where T: ArrowPrimitiveType {
    fn len(&self) -> usize {
        self.len as usize
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

struct StructArrayData {
    len: i32,
    //bitmap: Bitmap,
    data: Vec<Box<ArrowType>>
}

impl ArrowType for StructArrayData {
    fn get_type(&self) -> DataType {
        unimplemented!()
    }
}
impl ArrayData for StructArrayData {
    fn len(&self) -> usize {
        self.len as usize
    }
}

/// Top level array type, just a holder for a boxed trait for the data it contains
pub struct Array {
    data: Box<ArrayData>
}

impl Array {
    fn len(&self) -> usize {
        self.data.len()
    }
}

/// Create an Array from a Vec<T> of primitive values
impl<T> From<Vec<T>> for Array where T: ArrowPrimitiveType + 'static {
    fn from(vec: Vec<T>) -> Self {
        let data: Box<ArrayData> = Box::new(BufferArrayData::from(Buffer::from(vec)));
        Array { data }
    }
}

///// This method mostly just used for unit tests
//impl From<Vec<&'static str>> for Array {
//    fn from(v: Vec<&'static str>) -> Self {
//        Array::from(v.iter().map(|s| s.to_string()).collect::<Vec<String>>())
//    }
//}
//
//impl From<Vec<String>> for Array {
//    fn from(v: Vec<String>) -> Self {
//        Array {
//            len: v.len() as i32,
//            null_count: 0,
//            validity_bitmap: None,
//            data: ArrayData::Utf8(List::from(v)),
//        }
//    }
//}
//
//impl From<Vec<Rc<Array>>> for Array {
//    fn from(v: Vec<Rc<Array>>) -> Self {
//        Array {
//            len: v.len() as i32,
//            null_count: 0,
//            validity_bitmap: None,
//            data: ArrayData::Struct(v.iter().map(|a| a.clone()).collect()),
//        }
//    }
//}
//
#[cfg(test)]
mod tests {
    use super::super::datatypes::*;
    use super::*;

//    #[test]
//    fn test_utf8_offsets() {
//        let a = Array::from(vec!["this", "is", "a", "test"]);
//        assert_eq!(4, a.len());
//        match *a.data() {
//            ArrayData::Utf8(ref list) => {
//                assert_eq!(11, list.data().len());
//                assert_eq!(0, *list.offsets().get(0));
//                assert_eq!(4, *list.offsets().get(1));
//                assert_eq!(6, *list.offsets().get(2));
//                assert_eq!(7, *list.offsets().get(3));
//                assert_eq!(11, *list.offsets().get(4));
//            }
//            _ => panic!(),
//        }
//    }
//
//    #[test]
//    fn test_utf8_slices() {
//        let a = Array::from(vec!["this", "is", "a", "test"]);
//        match *a.data() {
//            ArrayData::Utf8(ref d) => {
//                assert_eq!(4, d.len());
//                assert_eq!("this", str::from_utf8(d.slice(0)).unwrap());
//                assert_eq!("is", str::from_utf8(d.slice(1)).unwrap());
//                assert_eq!("a", str::from_utf8(d.slice(2)).unwrap());
//                assert_eq!("test", str::from_utf8(d.slice(3)).unwrap());
//            }
//            _ => panic!(),
//        }
//    }
//
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

//    #[test]
//    fn test_from_i32() {
//        let a = Array::from(vec![15, 14, 13, 12, 11]);
//        assert_eq!(5, a.len());
//        match *a.data() {
//            ArrayData::Int32(ref b) => {
//                assert_eq!(vec![15, 14, 13, 12, 11], b.iter().collect::<Vec<i32>>());
//            }
//            _ => panic!(),
//        }
//    }

    #[test]
    fn test_from_empty_vec() {
        let v: Vec<i32> = vec![];
        let a = Array::from(v);
        assert_eq!(0, a.len());
    }

//    #[test]
//    fn test_from_optional_i32() {
//        let a = Array::from(vec![Some(1), None, Some(2), Some(3), None]);
//        assert_eq!(5, a.len());
//        // 1 == not null
//        let validity_bitmap = a.validity_bitmap.unwrap();
//        assert_eq!(true, validity_bitmap.is_set(0));
//        assert_eq!(false, validity_bitmap.is_set(1));
//        assert_eq!(true, validity_bitmap.is_set(2));
//        assert_eq!(true, validity_bitmap.is_set(3));
//        assert_eq!(false, validity_bitmap.is_set(4));
//    }
//
//    #[test]
//    fn test_struct() {
//        let _schema = DataType::Struct(vec![
//            Field::new("a", DataType::Int32, false),
//            Field::new("b", DataType::Float32, false),
//        ]);
//
//        let a = Rc::new(Array::from(vec![1, 2, 3, 4, 5]));
//        let b = Rc::new(Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]));
//        let _ = Rc::new(Array::from(vec![a, b]));
//    }
//
}
