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

//! Arrow Tensor Type
use std::marker::PhantomData;
use std::mem;

use buffer::Buffer;
use datatypes::{ArrowPrimitiveType, DataType};

/// Computes the strides required assuming a row major memory layout
fn compute_row_major_strides<T>(shape: &Vec<i64>) -> Vec<i64>
where
    T: ArrowPrimitiveType,
{
    let mut remaining_bytes = mem::size_of::<T>();
    for i in shape {
        remaining_bytes = remaining_bytes
            .checked_mul(*i as usize)
            .expect("Overflow occurred when computing row major strides.");
    }

    let mut strides = Vec::<i64>::new();
    for i in shape {
        remaining_bytes /= *i as usize;
        strides.push(remaining_bytes as i64);
    }
    strides
}

/// Computes the strides required assuming a column major memory layout
fn compute_column_major_strides<T>(shape: &Vec<i64>) -> Vec<i64>
where
    T: ArrowPrimitiveType,
{
    let mut remaining_bytes = mem::size_of::<T>();
    let mut strides = Vec::<i64>::new();
    for i in shape {
        strides.push(remaining_bytes as i64);
        remaining_bytes = remaining_bytes
            .checked_mul(*i as usize)
            .expect("Overflow occurred when computing column major strides.");
    }
    strides
}

/// Tensor of primitive types
pub struct Tensor<'a, T>
where
    T: ArrowPrimitiveType,
{
    data_type: DataType,
    buffer: Buffer,
    shape: Option<Vec<i64>>,
    strides: Option<Vec<i64>>,
    names: Option<Vec<&'a str>>,
    _marker: PhantomData<T>,
}

macro_rules! impl_tensor {
    ($data_ty:path, $native_ty:ident) => {
        impl<'a> Tensor<'a, $native_ty> {
            /// Creates a new `Tensor`
            pub fn new(
                buffer: Buffer,
                shape: Option<Vec<i64>>,
                strides: Option<Vec<i64>>,
                names: Option<Vec<&'a str>>,
            ) -> Self {
                match &shape {
                    None => {
                        assert_eq!(
                            buffer.len(),
                            mem::size_of::<$native_ty>(),
                            "underlying buffer should only contain a single tensor element"
                        );
                        assert_eq!(None, strides);
                        assert_eq!(None, names);
                    }
                    Some(ref s) => {
                        strides
                            .iter()
                            .map(|i| {
                                assert_eq!(s.len(), i.len(), "shape and stride dimensions differ")
                            }).next();
                        names
                            .iter()
                            .map(|i| {
                                assert_eq!(
                                    s.len(),
                                    i.len(),
                                    "number of dimensions and number of dimension names differ"
                                )
                            }).next();
                    }
                };
                Self {
                    data_type: $data_ty,
                    buffer,
                    shape,
                    strides,
                    names,
                    _marker: PhantomData,
                }
            }

            /// Creates a new Tensor using row major memory layout
            pub fn new_row_major(
                buffer: Buffer,
                shape: Option<Vec<i64>>,
                names: Option<Vec<&'a str>>,
            ) -> Self {
                let strides = match &shape {
                    None => None,
                    Some(ref s) => Some(compute_row_major_strides::<$native_ty>(&s)),
                };
                Self::new(buffer, shape, strides, names)
            }

            /// Creates a new Tensor using column major memory layout
            pub fn new_column_major(
                buffer: Buffer,
                shape: Option<Vec<i64>>,
                names: Option<Vec<&'a str>>,
            ) -> Self {
                let strides = match &shape {
                    None => None,
                    Some(ref s) => Some(compute_column_major_strides::<$native_ty>(&s)),
                };
                Self::new(buffer, shape, strides, names)
            }

            /// The data type of the `Tensor`
            pub fn data_type(&self) -> &DataType {
                &self.data_type
            }

            /// The sizes of the dimensions
            pub fn shape(&self) -> Option<&Vec<i64>> {
                self.shape.as_ref()
            }

            /// Returns a reference to the underlying `Buffer`
            pub fn data(&self) -> &Buffer {
                &self.buffer
            }

            /// The number of bytes between elements in each dimension
            pub fn strides(&self) -> Option<&Vec<i64>> {
                self.strides.as_ref()
            }

            /// The names of the dimensions
            pub fn names(&self) -> Option<&Vec<&'a str>> {
                self.names.as_ref()
            }

            /// The number of dimensions
            pub fn ndim(&self) -> i64 {
                match &self.shape {
                    None => 0,
                    Some(v) => v.len() as i64,
                }
            }

            /// The name of dimension i
            pub fn dim_name(&self, i: i64) -> Option<&'a str> {
                match &self.names {
                    None => None,
                    Some(ref names) => Some(&names[i as usize]),
                }
            }

            /// The total number of elements in the `Tensor`
            pub fn size(&self) -> i64 {
                (self.buffer.len() / mem::size_of::<$native_ty>()) as i64
            }

            /// Indicates if the data is laid out contiguously in memory
            pub fn is_contiguous(&self) -> bool {
                self.is_row_major() || self.is_column_major()
            }

            /// Indicates if the memory layout row major
            pub fn is_row_major(&self) -> bool {
                match self.shape {
                    None => false,
                    Some(ref s) => Some(compute_row_major_strides::<$native_ty>(s)) == self.strides,
                }
            }

            /// Indicates if the memory layout column major
            pub fn is_column_major(&self) -> bool {
                match self.shape {
                    None => false,
                    Some(ref s) => {
                        Some(compute_column_major_strides::<$native_ty>(s)) == self.strides
                    }
                }
            }
        }
    };
}

impl_tensor!(DataType::UInt8, u8);
impl_tensor!(DataType::UInt16, u16);
impl_tensor!(DataType::UInt32, u32);
impl_tensor!(DataType::UInt64, u64);
impl_tensor!(DataType::Int8, i8);
impl_tensor!(DataType::Int16, i16);
impl_tensor!(DataType::Int32, i32);
impl_tensor!(DataType::Int64, i64);
impl_tensor!(DataType::Float32, f32);
impl_tensor!(DataType::Float64, f64);

#[cfg(test)]
mod tests {
    use super::*;
    use buffer::Buffer;
    use builder::BufferBuilder;

    #[test]
    fn test_compute_row_major_strides() {
        assert_eq!(
            vec![48, 8],
            compute_row_major_strides::<i64>(&vec![4_i64, 6])
        );
        assert_eq!(
            vec![24, 4],
            compute_row_major_strides::<i32>(&vec![4_i64, 6])
        );
        assert_eq!(vec![6, 1], compute_row_major_strides::<i8>(&vec![4_i64, 6]));
    }

    #[test]
    fn test_compute_column_major_strides() {
        assert_eq!(
            vec![8, 32],
            compute_column_major_strides::<i64>(&vec![4_i64, 6])
        );
        assert_eq!(
            vec![4, 16],
            compute_column_major_strides::<i32>(&vec![4_i64, 6])
        );
        assert_eq!(
            vec![1, 4],
            compute_column_major_strides::<i8>(&vec![4_i64, 6])
        );
    }

    #[test]
    fn test_zero_dim() {
        let buf = Buffer::from(&[1]);
        let tensor = Tensor::<u8>::new(buf, None, None, None);
        assert_eq!(1, tensor.size());
        assert_eq!(None, tensor.shape());
        assert_eq!(None, tensor.names());
        assert_eq!(0, tensor.ndim());
        assert_eq!(false, tensor.is_row_major());
        assert_eq!(false, tensor.is_column_major());
        assert_eq!(false, tensor.is_contiguous());

        let buf = Buffer::from(&[1, 2, 2, 2]);
        let tensor = Tensor::<i32>::new(buf, None, None, None);
        assert_eq!(1, tensor.size());
        assert_eq!(None, tensor.shape());
        assert_eq!(None, tensor.names());
        assert_eq!(0, tensor.ndim());
        assert_eq!(false, tensor.is_row_major());
        assert_eq!(false, tensor.is_column_major());
        assert_eq!(false, tensor.is_contiguous());
    }

    #[test]
    fn test_tensor() {
        let mut builder = BufferBuilder::<i32>::new(16);
        for i in 0..16 {
            builder.push(i).unwrap();
        }
        let buf = builder.finish();
        let tensor = Tensor::<i32>::new(buf, Some(vec![2, 8]), None, None);
        assert_eq!(16, tensor.size());
        assert_eq!(Some(vec![2_i64, 8]).as_ref(), tensor.shape());
        assert_eq!(None, tensor.strides());
        assert_eq!(2, tensor.ndim());
        assert_eq!(None, tensor.names());
    }

    #[test]
    fn test_new_row_major() {
        let mut builder = BufferBuilder::<i32>::new(16);
        for i in 0..16 {
            builder.push(i).unwrap();
        }
        let buf = builder.finish();
        let tensor = Tensor::<i32>::new_row_major(buf, Some(vec![2, 8]), None);
        assert_eq!(16, tensor.size());
        assert_eq!(Some(vec![2_i64, 8]).as_ref(), tensor.shape());
        assert_eq!(Some(vec![32_i64, 4]).as_ref(), tensor.strides());
        assert_eq!(None, tensor.names());
        assert_eq!(2, tensor.ndim());
        assert_eq!(true, tensor.is_row_major());
        assert_eq!(false, tensor.is_column_major());
        assert_eq!(true, tensor.is_contiguous());
    }

    #[test]
    fn test_new_column_major() {
        let mut builder = BufferBuilder::<i32>::new(16);
        for i in 0..16 {
            builder.push(i).unwrap();
        }
        let buf = builder.finish();
        let tensor = Tensor::<i32>::new_column_major(buf, Some(vec![2, 8]), None);
        assert_eq!(16, tensor.size());
        assert_eq!(Some(vec![2_i64, 8]).as_ref(), tensor.shape());
        assert_eq!(Some(vec![4_i64, 8]).as_ref(), tensor.strides());
        assert_eq!(None, tensor.names());
        assert_eq!(2, tensor.ndim());
        assert_eq!(false, tensor.is_row_major());
        assert_eq!(true, tensor.is_column_major());
        assert_eq!(true, tensor.is_contiguous());
    }

    #[test]
    fn test_with_names() {
        let mut builder = BufferBuilder::<i64>::new(8);
        for i in 0..8 {
            builder.push(i).unwrap();
        }
        let buf = builder.finish();
        let names = vec!["Dim 1", "Dim 2"];
        let tensor = Tensor::<i64>::new_column_major(buf, Some(vec![2, 4]), Some(names));
        assert_eq!(8, tensor.size());
        assert_eq!(Some(vec![2_i64, 4]).as_ref(), tensor.shape());
        assert_eq!(Some(vec![8_i64, 16]).as_ref(), tensor.strides());
        assert_eq!("Dim 1", tensor.dim_name(0).unwrap());
        assert_eq!("Dim 2", tensor.dim_name(1).unwrap());
        assert_eq!(2, tensor.ndim());
        assert_eq!(false, tensor.is_row_major());
        assert_eq!(true, tensor.is_column_major());
        assert_eq!(true, tensor.is_contiguous());
    }

    #[test]
    #[should_panic(expected = "shape and stride dimensions differ")]
    fn test_inconsistent_strides() {
        let mut builder = BufferBuilder::<i32>::new(16);
        for i in 0..16 {
            builder.push(i).unwrap();
        }
        let buf = builder.finish();
        Tensor::<i32>::new(buf, Some(vec![2, 8]), Some(vec![2, 8, 1]), None);
    }

    #[test]
    #[should_panic(expected = "number of dimensions and number of dimension names differ")]
    fn test_inconsistent_names() {
        let mut builder = BufferBuilder::<i32>::new(16);
        for i in 0..16 {
            builder.push(i).unwrap();
        }
        let buf = builder.finish();
        Tensor::<i32>::new(
            buf,
            Some(vec![2, 8]),
            Some(vec![4, 8]),
            Some(vec!["1", "2", "3"]),
        );
    }
}
