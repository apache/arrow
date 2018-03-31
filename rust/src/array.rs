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
use super::error::*;

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
    Struct(Vec<Rc<Array>>)
}

macro_rules! arraydata_from_primitive {
    ($DT:ty, $AT:ident) => {
        impl From<Vec<$DT>> for ArrayData {
            fn from(v: Vec<$DT>) -> Self {
                ArrayData::$AT(Buffer::from(v))
            }
        }

    }
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
    pub len: i32,
    pub null_count: i32,
    pub validity_bitmap: Option<Bitmap>,
    pub data: ArrayData
}

impl Array {

    /// Create a new array where there are no null values
    pub fn new(len: usize, data: ArrayData) -> Self {
        Array { len: len as i32, data, validity_bitmap: None, null_count: 0 }
    }

    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

}

/// type-safe array operations
trait ArrayOps<T> {
    /// Get one element from an array. Note that this is an expensive call since it
    /// will pattern match the type of the array on every invocation. We should add
    /// other efficient iterator and map methods so we can perform columnar operations
    /// instead.
    fn get(&self, i: usize) -> Result<T,Error>;

    /// Compare two same-typed arrays using a boolean closure e.g. eq, gt, lt, and so on
    fn compare(&self, other: &Array, f: &Fn(T,T) -> bool) -> Result<Vec<bool>, Error>;

    /// Perform a computation on two same-typed arrays and produce a result of the same type e.g. c = a + b
    fn compute(&self, other: &Array, f: &Fn(T,T) -> T) -> Result<Vec<T>, Error>;
}

macro_rules! array_ops {
    ($DT:ty, $AT:ident) => {
        impl ArrayOps<$DT> for Array {
            fn get(&self, i: usize) -> Result<$DT,Error> {
                match self.data() {
                    &ArrayData::$AT(ref buf) => Ok(unsafe {*buf.data().offset(i as isize)}),
                    _ => Err(Error::from("Request for $DT but array is not $DT"))
                }
            }
            fn compare(&self, other: &Array, f: &Fn($DT,$DT) -> bool) -> Result<Vec<bool>, Error> {
                match (&self.data, &other.data) {
                    (&ArrayData::$AT(ref l), &ArrayData::$AT(ref r)) => {
                        let mut b: Vec<bool> = Vec::with_capacity(self.len as usize);
                        for i in 0..self.len as isize {
                            let lv : $DT = unsafe { *l.data().offset(i) };
                            let rv : $DT = unsafe { *r.data().offset(i) };
                            b.push(f(lv,rv));
                        }
                        Ok(b)
                    },
                    _ => Err(Error::from("Cannot compare arrays of this type"))
                }
            }
            fn compute(&self, other: &Array, f: &Fn($DT,$DT) -> $DT) -> Result<Vec<$DT>, Error> {
                match (&self.data, &other.data) {
                    (&ArrayData::$AT(ref l), &ArrayData::$AT(ref r)) => {
                        let mut b: Vec<$DT> = Vec::with_capacity(self.len as usize);
                        for i in 0..self.len as isize {
                            let lv : $DT = unsafe { *l.data().offset(i) };
                            let rv : $DT = unsafe { *r.data().offset(i) };
                            b.push(f(lv,rv));
                        }
                        Ok(b)
                    },
                    _ => Err(Error::from("Cannot compare arrays of this type"))
                }
            }
        }
    }
}

array_ops!(bool, Boolean);
array_ops!(f64, Float64);
array_ops!(f32, Float32);
array_ops!(u8, UInt8);
array_ops!(u16, UInt16);
array_ops!(u32, UInt32);
array_ops!(u64, UInt64);
array_ops!(i8, Int8);
array_ops!(i16, Int16);
array_ops!(i32, Int32);
array_ops!(i64, Int64);

macro_rules! array_from_primitive {
    ($DT:ty) => {
        impl From<Vec<$DT>> for Array {
            fn from(v: Vec<$DT>) -> Self {
                Array { len: v.len() as i32, null_count: 0, validity_bitmap: None, data: ArrayData::from(v) }
            }
        }
    }

}

array_from_primitive!(bool);
array_from_primitive!(f32);
array_from_primitive!(f64);
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
                for i in 0 .. v.len() {
                    if v[i].is_none() {
                        null_count+=1;
                        validity_bitmap.clear(i);
                    }
                }
                let values = v.iter().map(|x| x.unwrap_or($DEFAULT)).collect::<Vec<$DT>>();
                Array { len: values.len() as i32, null_count, validity_bitmap: Some(validity_bitmap), data: ArrayData::from(values) }
            }
        }
    }

}

array_from_optional_primitive!(bool, false);
array_from_optional_primitive!(f32, 0_f32);
array_from_optional_primitive!(f64, 0_f64);
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
            data: ArrayData::Utf8(List::from(v))
        }
    }
}

impl From<Vec<Rc<Array>>> for Array {
    fn from(v: Vec<Rc<Array>>) -> Self {
        Array {
            len: v.len() as i32,
            null_count: 0,
            validity_bitmap: None,
            data: ArrayData::Struct(v.iter().map(|a| a.clone()).collect())
        }
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
            &ArrayData::Utf8(List{ ref data, ref offsets }) => {
                assert_eq!(11, data.len());
                assert_eq!(0, *offsets.get(0));
                assert_eq!(4, *offsets.get(1));
                assert_eq!(6, *offsets.get(2));
                assert_eq!(7, *offsets.get(3));
                assert_eq!(11, *offsets.get(4));
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

        assert_eq!(15, a.get(0).unwrap());
        assert_eq!(14, a.get(1).unwrap());
        assert_eq!(13, a.get(2).unwrap());
        assert_eq!(12, a.get(3).unwrap());
        assert_eq!(11, a.get(4).unwrap());
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

    #[test]
    fn test_array_eq() {
        let a = Array::from(vec![1,2,3,4,5]);
        let b = Array::from(vec![5,4,3,2,1]);
        let c = a.compare(&b, &|a: i32,b: i32| a == b).unwrap();
        assert_eq!(c, vec![false,false,true,false,false]);
    }

    #[test]
    fn test_array_lt() {
        let a = Array::from(vec![1,2,3,4,5]);
        let b = Array::from(vec![5,4,3,2,1]);
        let c = a.compare(&b, &|a: i32,b: i32| a < b).unwrap();
        assert_eq!(c, vec![true,true,false,false,false]);
    }

    #[test]
    fn test_array_gt() {
        let a = Array::from(vec![1,2,3,4,5]);
        let b = Array::from(vec![5,4,3,2,1]);
        let c = a.compare(&b, &|a: i32,b: i32| a > b).unwrap();
        assert_eq!(c, vec![false,false,false,true,true]);
    }

    #[test]
    fn test_array_add() {
        let a = Array::from(vec![1,2,3,4,5]);
        let b = Array::from(vec![5,4,3,2,1]);
        let c = a.compute(&b, &|a: i32,b: i32| a + b).unwrap();
        assert_eq!(c, vec![6,6,6,6,6]);
    }

    #[test]
    fn test_array_multiply() {
        let a = Array::from(vec![1,2,3,4,5]);
        let b = Array::from(vec![5,4,3,2,1]);
        let c = a.compute(&b, &|a: i32,b: i32| a * b).unwrap();
        assert_eq!(c, vec![5,8,9,8,5]);
    }
}




