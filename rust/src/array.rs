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
use std::rc::Rc;
use std::str;
use std::string::String;

use super::bitmap::Bitmap;
use super::buffer::*;
use super::builder::*;
use super::datatypes::*;
use super::list::*;
use super::list_builder::*;

/// Array data type
pub trait ArrayData {
    fn len(&self) -> usize;
    fn null_count(&self) -> usize;
    fn validity_bitmap(&self) -> &Option<Bitmap>;
    fn as_any(&self) -> &Any;
}

/// Array of List<T>
pub struct ListArrayData<T: ArrowPrimitiveType> {
    len: usize,
    list: List<T>,
    null_count: usize,
    validity_bitmap: Option<Bitmap>,
}

impl<T> ListArrayData<T>
where
    T: ArrowPrimitiveType,
{
    pub fn get(&self, i: usize) -> &[T] {
        self.list.get(i)
    }

    pub fn list(&self) -> &List<T> {
        &self.list
    }
}

impl<T> From<List<T>> for ListArrayData<T>
where
    T: ArrowPrimitiveType,
{
    fn from(list: List<T>) -> Self {
        let len = list.len();
        ListArrayData {
            len,
            list,
            validity_bitmap: None,
            null_count: 0,
        }
    }
}

impl<T> ArrayData for ListArrayData<T>
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
pub struct BufferArrayData<T: ArrowPrimitiveType> {
    len: usize,
    buffer: Buffer<T>,
    null_count: usize,
    validity_bitmap: Option<Bitmap>,
}

impl<T> BufferArrayData<T>
where
    T: ArrowPrimitiveType,
{
    pub fn new(data: Buffer<T>, null_count: usize, validity_bitmap: Option<Bitmap>) -> Self {
        BufferArrayData {
            len: data.len(),
            buffer: data,
            null_count,
            validity_bitmap,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter(&self) -> BufferIterator<T> {
        self.buffer.iter()
    }

    pub fn buffer(&self) -> &Buffer<T> {
        &self.buffer
    }

    /// Determine the minimum value in the buffer
    pub fn min(&self) -> Option<T> {
        let mut n: Option<T> = None;
        match &self.validity_bitmap {
            &Some(ref bitmap) => for i in 0..self.len {
                if bitmap.is_set(i) {
                    let mut m = self.buffer.get(i);
                    match n {
                        None => n = Some(*m),
                        Some(nn) => if *m < nn {
                            n = Some(*m)
                        },
                    }
                }
            },
            &None => for i in 0..self.len {
                let mut m = self.buffer.get(i);
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

    /// Determine the maximum value in the buffer
    pub fn max(&self) -> Option<T> {
        let mut n: Option<T> = None;
        match &self.validity_bitmap {
            &Some(ref bitmap) => for i in 0..self.len {
                if bitmap.is_set(i) {
                    let mut m = self.buffer.get(i);
                    match n {
                        None => n = Some(*m),
                        Some(nn) => if *m > nn {
                            n = Some(*m)
                        },
                    }
                }
            },
            &None => for i in 0..self.len {
                let mut m = self.buffer.get(i);
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

impl<T> ArrayData for BufferArrayData<T>
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

impl<T> From<Buffer<T>> for BufferArrayData<T>
where
    T: ArrowPrimitiveType,
{
    fn from(data: Buffer<T>) -> Self {
        BufferArrayData {
            len: data.len(),
            buffer: data,
            validity_bitmap: None,
            null_count: 0,
        }
    }
}

pub struct StructArrayData {
    len: usize,
    columns: Vec<Rc<ArrayData>>,
    null_count: usize,
    validity_bitmap: Option<Bitmap>,
}

impl StructArrayData {
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }
    pub fn column(&self, i: usize) -> &Rc<ArrayData> {
        &self.columns[i]
    }
}

impl ArrayData for StructArrayData {
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

impl From<Vec<Rc<ArrayData>>> for StructArrayData {
    fn from(data: Vec<Rc<ArrayData>>) -> Self {
        StructArrayData {
            len: data[0].len(),
            columns: data,
            null_count: 0,
            validity_bitmap: None,
        }
    }
}

/// Top level array type, just a holder for a boxed trait for the data it contains
pub struct Array {
    data: Rc<ArrayData>,
}

impl Array {
    pub fn new(data: Rc<ArrayData>) -> Self {
        Array { data: data }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn null_count(&self) -> usize {
        self.data.null_count()
    }
    pub fn data(&self) -> &Rc<ArrayData> {
        &self.data
    }
    pub fn validity_bitmap(&self) -> &Option<Bitmap> {
        self.data.validity_bitmap()
    }
}

impl<T> From<Buffer<T>> for Array
where
    T: ArrowPrimitiveType + 'static,
{
    fn from(buffer: Buffer<T>) -> Self {
        Array::new(Rc::new(BufferArrayData::from(buffer)))
    }
}

impl<T> From<List<T>> for Array
where
    T: ArrowPrimitiveType + 'static,
{
    fn from(list: List<T>) -> Self {
        Array::new(Rc::new(ListArrayData::from(list)))
    }
}

/// Create an Array from a BufferArrayData<T> of primitive values
impl<T> From<BufferArrayData<T>> for Array
where
    T: ArrowPrimitiveType + 'static,
{
    fn from(data: BufferArrayData<T>) -> Self {
        Array::new(Rc::new(data))
    }
}

/// Create an Array from a Vec<T> of primitive values
impl<T> From<Vec<T>> for Array
where
    T: ArrowPrimitiveType + 'static,
{
    fn from(vec: Vec<T>) -> Self {
        Array::new(Rc::new(BufferArrayData::from(Buffer::from(vec))))
    }
}

/// Create an Array from a Vec<T> of primitive values
impl<T> From<Vec<Option<T>>> for Array
where
    T: ArrowPrimitiveType + 'static,
{
    fn from(vec: Vec<Option<T>>) -> Self {
        Array::from(BufferArrayData::from(vec))
    }
}

/// Create a BufferArrayData<T> from a Vec<Option<T>> of primitive values
impl<T> From<Vec<Option<T>>> for BufferArrayData<T>
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
        BufferArrayData::new(builder.finish(), null_count, Some(validity_bitmap))
    }
}

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
    use super::*;

    #[test]
    fn array_data_from_list_u8() {
        let mut b: ListBuilder<u8> = ListBuilder::new();
        b.push(&[1, 2, 3, 4, 5]);
        b.push(&[5, 4, 3, 2, 1]);
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
        let array_list_u8 = array
            .data()
            .as_any()
            .downcast_ref::<ListArrayData<u8>>()
            .unwrap();
        assert_eq!(2, array_list_u8.len());
        assert_eq!("Hello, ", str::from_utf8(array_list_u8.get(0)).unwrap());
        assert_eq!("World!", str::from_utf8(array_list_u8.get(1)).unwrap());
    }

    #[test]
    fn test_from_bool() {
        let a = Array::from(vec![false, false, true, false]);
        assert_eq!(4, a.len());
        assert_eq!(0, a.null_count());
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
        let a: Rc<ArrayData> = Rc::new(BufferArrayData::from(Buffer::from(vec![1, 2, 3, 4, 5])));
        let b: Rc<ArrayData> = Rc::new(BufferArrayData::from(Buffer::from(vec![
            1.1, 2.2, 3.3, 4.4, 5.5
        ])));

        let s = StructArrayData::from(vec![a, b]);
        assert_eq!(2, s.num_columns());
        assert_eq!(0, s.null_count());
    }

    #[test]
    fn test_buffer_array_min_max() {
        let a = BufferArrayData::from(Buffer::from(vec![5, 6, 7, 8, 9]));
        assert_eq!(5, a.min().unwrap());
        assert_eq!(9, a.max().unwrap());
    }

    #[test]
    fn test_buffer_array_min_max_with_nulls() {
        let a = BufferArrayData::from(vec![Some(5), None, None, Some(8), Some(9)]);
        assert_eq!(5, a.min().unwrap());
        assert_eq!(9, a.max().unwrap());
    }

}
