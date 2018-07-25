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

///! Array types
use std::any::Any;
use std::convert::From;
use std::ops::Add;
use std::sync::Arc;
use std::str;
use std::string::String;

use super::bitmap::Bitmap;
use super::buffer::*;
use super::builder::*;
use super::datatypes::*;
use super::list::*;
use super::list_builder::*;

/// Trait for dealing with different types of Array at runtime when the type of the
/// array is not known in advance
pub trait Array: Send + Sync {
    /// Returns the length of the array (number of items in the array)
    fn len(&self) -> usize;
    /// Returns the number of null values in the array
    fn null_count(&self) -> usize;
    /// Optional validity bitmap (can be None if there are no null values)
    fn validity_bitmap(&self) -> &Option<Bitmap>;
    /// Return the array as Any so that it can be downcast to a specific implementation
    fn as_any(&self) -> &Any;
}

/// Array of List<T>
pub struct ListArray<T: ArrowPrimitiveType> {
    len: usize,
    data: List<T>,
    null_count: usize,
    validity_bitmap: Option<Bitmap>,
}

impl<T> ListArray<T>
where
    T: ArrowPrimitiveType,
{
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn null_count(&self) -> usize {
        self.null_count
    }

    pub fn validity_bitmap(&self) -> &Option<Bitmap> {
        &self.validity_bitmap
    }

    pub fn get(&self, i: usize) -> &[T] {
        self.data.get(i)
    }

    pub fn list(&self) -> &List<T> {
        &self.data
    }
}

/// Create a ListArray<T> from a List<T> without null values
impl<T> From<List<T>> for ListArray<T>
where
    T: ArrowPrimitiveType,
{
    fn from(list: List<T>) -> Self {
        let len = list.len();
        ListArray {
            len,
            data: list,
            validity_bitmap: None,
            null_count: 0,
        }
    }
}

/// Create ListArray<u8> from Vec<&'static str>
impl From<Vec<&'static str>> for ListArray<u8> {
    fn from(v: Vec<&'static str>) -> Self {
        let mut builder: ListBuilder<u8> = ListBuilder::with_capacity(v.len());
        for s in v {
            builder.push(s.as_bytes())
        }
        ListArray::from(builder.finish())
    }
}

/// Create ListArray<u8> from Vec<String>
impl From<Vec<String>> for ListArray<u8> {
    fn from(v: Vec<String>) -> Self {
        let mut builder: ListBuilder<u8> = ListBuilder::with_capacity(v.len());
        for s in v {
            builder.push(s.as_bytes())
        }
        ListArray::from(builder.finish())
    }
}

impl<T> Array for ListArray<T>
where
    T: ArrowPrimitiveType,
{
    fn len(&self) -> usize {
        self.len
    }
    fn null_count(&self) -> usize {
        self.null_count
    }
    fn validity_bitmap(&self) -> &Option<Bitmap> {
        &self.validity_bitmap
    }
    fn as_any(&self) -> &Any {
        self
    }
}

/// Array of T
pub struct PrimitiveArray<T: ArrowPrimitiveType> {
    len: usize,
    data: Buffer<T>,
    null_count: usize,
    validity_bitmap: Option<Bitmap>,
}

impl<T> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    pub fn new(data: Buffer<T>, null_count: usize, validity_bitmap: Option<Bitmap>) -> Self {
        PrimitiveArray {
            len: data.len(),
            data: data,
            null_count,
            validity_bitmap,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, i: usize) -> &T {
        self.data.get(i)
    }

    pub fn iter(&self) -> BufferIterator<T> {
        self.data.iter()
    }

    pub fn buffer(&self) -> &Buffer<T> {
        &self.data
    }

    /// Determine the minimum value in the array
    pub fn min(&self) -> Option<T> {
        let mut n: Option<T> = None;
        match &self.validity_bitmap {
            &Some(ref bitmap) => for i in 0..self.len {
                if bitmap.is_set(i) {
                    let mut m = self.data.get(i);
                    match n {
                        None => n = Some(*m),
                        Some(nn) => if *m < nn {
                            n = Some(*m)
                        },
                    }
                }
            },
            &None => for i in 0..self.len {
                let mut m = self.data.get(i);
                match n {
                    None => n = Some(*m),
                    Some(nn) => if *m < nn {
                        n = Some(*m)
                    },
                }
            },
        }
        n
    }

    /// Determine the maximum value in the array
    pub fn max(&self) -> Option<T> {
        let mut n: Option<T> = None;
        match &self.validity_bitmap {
            &Some(ref bitmap) => for i in 0..self.len {
                if bitmap.is_set(i) {
                    let mut m = self.data.get(i);
                    match n {
                        None => n = Some(*m),
                        Some(nn) => if *m > nn {
                            n = Some(*m)
                        },
                    }
                }
            },
            &None => for i in 0..self.len {
                let mut m = self.data.get(i);
                match n {
                    None => n = Some(*m),
                    Some(nn) => if *m > nn {
                        n = Some(*m)
                    },
                }
            },
        }
        n
    }
}

/// Implement the Add operation for types that support Add
impl<T> PrimitiveArray<T>
where
    T: ArrowPrimitiveType + Add<Output = T>,
{
    pub fn add(&self, other: &PrimitiveArray<T>) -> PrimitiveArray<T> {
        let mut builder: Builder<T> = Builder::new();
        for i in 0..self.len {
            let x = *self.data.get(i) + *other.data.get(i);
            builder.push(x);
        }
        PrimitiveArray::from(builder.finish())
    }
}

impl<T> Array for PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    fn len(&self) -> usize {
        self.len
    }
    fn null_count(&self) -> usize {
        self.null_count
    }
    fn validity_bitmap(&self) -> &Option<Bitmap> {
        &self.validity_bitmap
    }
    fn as_any(&self) -> &Any {
        self
    }
}

/// Create a BufferArray<T> from a Buffer<T> without null values
impl<T> From<Buffer<T>> for PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    fn from(data: Buffer<T>) -> Self {
        PrimitiveArray {
            len: data.len(),
            data: data,
            validity_bitmap: None,
            null_count: 0,
        }
    }
}

/// Create a BufferArray<T> from a Vec<T> of primitive values
impl<T> From<Vec<T>> for PrimitiveArray<T>
where
    T: ArrowPrimitiveType + 'static,
{
    fn from(vec: Vec<T>) -> Self {
        PrimitiveArray::from(Buffer::from(vec))
    }
}

/// Create a BufferArray<T> from a Vec<Optional<T>> with null handling
impl<T> From<Vec<Option<T>>> for PrimitiveArray<T>
where
    T: ArrowPrimitiveType + 'static,
{
    fn from(v: Vec<Option<T>>) -> Self {
        let mut builder: Builder<T> = Builder::with_capacity(v.len());
        builder.set_len(v.len());
        let mut null_count = 0;
        let mut validity_bitmap = Bitmap::new(v.len());
        for i in 0..v.len() {
            match v[i] {
                Some(value) => builder.set(i, value),
                None => {
                    null_count += 1;
                    validity_bitmap.clear(i);
                }
            }
        }
        PrimitiveArray::new(builder.finish(), null_count, Some(validity_bitmap))
    }
}

/// An Array of structs
pub struct StructArray {
    len: usize,
    columns: Vec<Arc<Array>>,
    null_count: usize,
    validity_bitmap: Option<Bitmap>,
}

impl StructArray {
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
    pub fn column(&self, i: usize) -> &Arc<Array> {
        &self.columns[i]
    }
}

impl Array for StructArray {
    fn len(&self) -> usize {
        self.len
    }
    fn null_count(&self) -> usize {
        self.null_count
    }
    fn validity_bitmap(&self) -> &Option<Bitmap> {
        &self.validity_bitmap
    }
    fn as_any(&self) -> &Any {
        self
    }
}

/// Create a StructArray from a list of arrays representing the fields of the struct. The fields
/// must be in the same order as the schema defining the struct.
impl From<Vec<Arc<Array>>> for StructArray {
    fn from(data: Vec<Arc<Array>>) -> Self {
        StructArray {
            len: data[0].len(),
            columns: data,
            null_count: 0,
            validity_bitmap: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn array_data_from_list_u8() {
        let mut b: ListBuilder<u8> = ListBuilder::new();
        b.push(&[1, 2, 3, 4, 5]);
        b.push(&[5, 4, 3, 2, 1]);
        let array_data = ListArray::from(b.finish());
        assert_eq!(2, array_data.len());
    }

    #[test]
    fn array_from_list_u8() {
        let mut b: ListBuilder<u8> = ListBuilder::new();
        b.push("Hello, ".as_bytes());
        b.push("World!".as_bytes());
        let array = ListArray::from(b.finish());
        // downcast back to the data
        let array_list_u8 = array.as_any().downcast_ref::<ListArray<u8>>().unwrap();
        assert_eq!(2, array_list_u8.len());
        assert_eq!("Hello, ", str::from_utf8(array_list_u8.get(0)).unwrap());
        assert_eq!("World!", str::from_utf8(array_list_u8.get(1)).unwrap());
    }

    #[test]
    fn test_from_bool() {
        let a = PrimitiveArray::from(vec![false, false, true, false]);
        assert_eq!(4, a.len());
        assert_eq!(0, a.null_count());
    }

    #[test]
    fn test_from_f32() {
        let a = PrimitiveArray::from(vec![1.23, 2.34, 3.45, 4.56]);
        assert_eq!(4, a.len());
    }

    #[test]
    fn test_from_i32() {
        let a = PrimitiveArray::from(vec![15, 14, 13, 12, 11]);
        assert_eq!(5, a.len());
    }

    #[test]
    fn test_from_empty_vec() {
        let v: Vec<i32> = vec![];
        let a = PrimitiveArray::from(v);
        assert_eq!(0, a.len());
    }

    #[test]
    fn test_from_optional_i32() {
        let a = PrimitiveArray::from(vec![Some(1), None, Some(2), Some(3), None]);
        assert_eq!(5, a.len());
        assert_eq!(2, a.null_count());
        // 1 == not null
        match a.validity_bitmap() {
            &Some(ref validity_bitmap) => {
                assert_eq!(true, validity_bitmap.is_set(0));
                assert_eq!(false, validity_bitmap.is_set(1));
                assert_eq!(true, validity_bitmap.is_set(2));
                assert_eq!(true, validity_bitmap.is_set(3));
                assert_eq!(false, validity_bitmap.is_set(4));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_struct() {
        let a: Arc<Array> = Arc::new(PrimitiveArray::from(Buffer::from(vec![1, 2, 3, 4, 5])));
        let b: Arc<Array> = Arc::new(PrimitiveArray::from(Buffer::from(vec![
            1.1, 2.2, 3.3, 4.4, 5.5,
        ])));

        let s = StructArray::from(vec![a, b]);
        assert_eq!(2, s.num_columns());
        assert_eq!(0, s.null_count());
    }

    #[test]
    fn test_buffer_array_min_max() {
        let a = PrimitiveArray::from(Buffer::from(vec![5, 6, 7, 8, 9]));
        assert_eq!(5, a.min().unwrap());
        assert_eq!(9, a.max().unwrap());
    }

    #[test]
    fn test_buffer_array_min_max_with_nulls() {
        let a = PrimitiveArray::from(vec![Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, a.min().unwrap());
        assert_eq!(9, a.max().unwrap());
    }

    #[test]
    fn test_access_array_concurrently() {
        let a = PrimitiveArray::from(Buffer::from(vec![5, 6, 7, 8, 9]));

        let ret = thread::spawn(move || {
            a.iter().collect::<Vec<i32>>()
        }).join();

        assert!(ret.is_ok());
        assert_eq!(vec![5, 6, 7, 8, 9], ret.ok().unwrap());
    }
}

