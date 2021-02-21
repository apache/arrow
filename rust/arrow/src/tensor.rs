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

//! Arrow Tensor Type, defined in
//! [`format/Tensor.fbs`](https://github.com/apache/arrow/blob/master/format/Tensor.fbs).

use std::marker::PhantomData;
use std::mem;

use crate::buffer::Buffer;
use crate::datatypes::*;

use crate::error::{ArrowError, Result};

/// Computes the strides required assuming a row major memory layout
fn compute_row_major_strides<T: ArrowPrimitiveType>(
    shape: &[usize],
) -> Result<Vec<usize>> {
    let mut remaining_bytes = mem::size_of::<T::Native>();

    for i in shape {
        if let Some(val) = remaining_bytes.checked_mul(*i) {
            remaining_bytes = val;
        } else {
            return Err(ArrowError::ComputeError(
                "overflow occurred when computing row major strides.".to_string(),
            ));
        }
    }

    let mut strides = Vec::<usize>::new();
    for i in shape {
        remaining_bytes /= *i;
        strides.push(remaining_bytes);
    }

    Ok(strides)
}

/// Computes the strides required assuming a column major memory layout
fn compute_column_major_strides<T: ArrowPrimitiveType>(
    shape: &[usize],
) -> Result<Vec<usize>> {
    let mut remaining_bytes = mem::size_of::<T::Native>();
    let mut strides = Vec::<usize>::new();

    for i in shape {
        strides.push(remaining_bytes);

        if let Some(val) = remaining_bytes.checked_mul(*i) {
            remaining_bytes = val;
        } else {
            return Err(ArrowError::ComputeError(
                "overflow occurred when computing column major strides.".to_string(),
            ));
        }
    }

    Ok(strides)
}

/// Tensor of primitive types
#[derive(Debug)]
pub struct Tensor<'a, T: ArrowPrimitiveType> {
    data_type: DataType,
    buffer: Buffer,
    shape: Option<Vec<usize>>,
    strides: Option<Vec<usize>>,
    names: Option<Vec<&'a str>>,
    _marker: PhantomData<T>,
}

pub type BooleanTensor<'a> = Tensor<'a, BooleanType>;
pub type Int8Tensor<'a> = Tensor<'a, Int8Type>;
pub type Int16Tensor<'a> = Tensor<'a, Int16Type>;
pub type Int32Tensor<'a> = Tensor<'a, Int32Type>;
pub type Int64Tensor<'a> = Tensor<'a, Int64Type>;
pub type UInt8Tensor<'a> = Tensor<'a, UInt8Type>;
pub type UInt16Tensor<'a> = Tensor<'a, UInt16Type>;
pub type UInt32Tensor<'a> = Tensor<'a, UInt32Type>;
pub type UInt64Tensor<'a> = Tensor<'a, UInt64Type>;
pub type Float32Tensor<'a> = Tensor<'a, Float32Type>;
pub type Float64Tensor<'a> = Tensor<'a, Float64Type>;

impl<'a, T: ArrowPrimitiveType> Tensor<'a, T> {
    /// Creates a new `Tensor`
    pub fn try_new(
        buffer: Buffer,
        shape: Option<Vec<usize>>,
        strides: Option<Vec<usize>>,
        names: Option<Vec<&'a str>>,
    ) -> Result<Self> {
        match shape {
            None => {
                if buffer.len() != mem::size_of::<T::Native>() {
                    return Err(ArrowError::InvalidArgumentError(
                        "underlying buffer should only contain a single tensor element"
                            .to_string(),
                    ));
                }

                if strides != None {
                    return Err(ArrowError::InvalidArgumentError(
                        "expected None strides for tensor with no shape".to_string(),
                    ));
                }

                if names != None {
                    return Err(ArrowError::InvalidArgumentError(
                        "expected None names for tensor with no shape".to_string(),
                    ));
                }
            }

            Some(ref s) => {
                if let Some(ref st) = strides {
                    if st.len() != s.len() {
                        return Err(ArrowError::InvalidArgumentError(
                            "shape and stride dimensions differ".to_string(),
                        ));
                    }
                }

                if let Some(ref n) = names {
                    if n.len() != s.len() {
                        return Err(ArrowError::InvalidArgumentError(
                            "number of dimensions and number of dimension names differ"
                                .to_string(),
                        ));
                    }
                }

                let total_elements: usize = s.iter().product();
                if total_elements != (buffer.len() / mem::size_of::<T::Native>()) {
                    return Err(ArrowError::InvalidArgumentError(
                        "number of elements in buffer does not match dimensions"
                            .to_string(),
                    ));
                }
            }
        };

        // Checking that the tensor strides used for construction are correct
        // otherwise a row major stride is calculated and used as value for the tensor
        let tensor_strides = {
            if let Some(st) = strides {
                if let Some(ref s) = shape {
                    if compute_row_major_strides::<T>(s)? == st
                        || compute_column_major_strides::<T>(s)? == st
                    {
                        Some(st)
                    } else {
                        return Err(ArrowError::InvalidArgumentError(
                            "the input stride does not match the selected shape"
                                .to_string(),
                        ));
                    }
                } else {
                    Some(st)
                }
            } else if let Some(ref s) = shape {
                Some(compute_row_major_strides::<T>(s)?)
            } else {
                None
            }
        };

        Ok(Self {
            data_type: T::DATA_TYPE,
            buffer,
            shape,
            strides: tensor_strides,
            names,
            _marker: PhantomData,
        })
    }

    /// Creates a new Tensor using row major memory layout
    pub fn new_row_major(
        buffer: Buffer,
        shape: Option<Vec<usize>>,
        names: Option<Vec<&'a str>>,
    ) -> Result<Self> {
        if let Some(ref s) = shape {
            let strides = Some(compute_row_major_strides::<T>(&s)?);

            Self::try_new(buffer, shape, strides, names)
        } else {
            Err(ArrowError::InvalidArgumentError(
                "shape required to create row major tensor".to_string(),
            ))
        }
    }

    /// Creates a new Tensor using column major memory layout
    pub fn new_column_major(
        buffer: Buffer,
        shape: Option<Vec<usize>>,
        names: Option<Vec<&'a str>>,
    ) -> Result<Self> {
        if let Some(ref s) = shape {
            let strides = Some(compute_column_major_strides::<T>(&s)?);

            Self::try_new(buffer, shape, strides, names)
        } else {
            Err(ArrowError::InvalidArgumentError(
                "shape required to create column major tensor".to_string(),
            ))
        }
    }

    /// The data type of the `Tensor`
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// The sizes of the dimensions
    pub fn shape(&self) -> Option<&Vec<usize>> {
        self.shape.as_ref()
    }

    /// Returns a reference to the underlying `Buffer`
    pub fn data(&self) -> &Buffer {
        &self.buffer
    }

    /// The number of bytes between elements in each dimension
    pub fn strides(&self) -> Option<&Vec<usize>> {
        self.strides.as_ref()
    }

    /// The names of the dimensions
    pub fn names(&self) -> Option<&Vec<&'a str>> {
        self.names.as_ref()
    }

    /// The number of dimensions
    pub fn ndim(&self) -> usize {
        match &self.shape {
            None => 0,
            Some(v) => v.len(),
        }
    }

    /// The name of dimension i
    pub fn dim_name(&self, i: usize) -> Option<&'a str> {
        self.names.as_ref().map(|ref names| names[i])
    }

    /// The total number of elements in the `Tensor`
    pub fn size(&self) -> usize {
        match self.shape {
            None => 0,
            Some(ref s) => s.iter().product(),
        }
    }

    /// Indicates if the data is laid out contiguously in memory
    pub fn is_contiguous(&self) -> Result<bool> {
        Ok(self.is_row_major()? || self.is_column_major()?)
    }

    /// Indicates if the memory layout row major
    pub fn is_row_major(&self) -> Result<bool> {
        match self.shape {
            None => Ok(false),
            Some(ref s) => Ok(Some(compute_row_major_strides::<T>(s)?) == self.strides),
        }
    }

    /// Indicates if the memory layout column major
    pub fn is_column_major(&self) -> Result<bool> {
        match self.shape {
            None => Ok(false),
            Some(ref s) => {
                Ok(Some(compute_column_major_strides::<T>(s)?) == self.strides)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::*;
    use crate::buffer::Buffer;

    #[test]
    fn test_compute_row_major_strides() {
        assert_eq!(
            vec![48_usize, 8],
            compute_row_major_strides::<Int64Type>(&[4_usize, 6]).unwrap()
        );
        assert_eq!(
            vec![24_usize, 4],
            compute_row_major_strides::<Int32Type>(&[4_usize, 6]).unwrap()
        );
        assert_eq!(
            vec![6_usize, 1],
            compute_row_major_strides::<Int8Type>(&[4_usize, 6]).unwrap()
        );
    }

    #[test]
    fn test_compute_column_major_strides() {
        assert_eq!(
            vec![8_usize, 32],
            compute_column_major_strides::<Int64Type>(&[4_usize, 6]).unwrap()
        );
        assert_eq!(
            vec![4_usize, 16],
            compute_column_major_strides::<Int32Type>(&[4_usize, 6]).unwrap()
        );
        assert_eq!(
            vec![1_usize, 4],
            compute_column_major_strides::<Int8Type>(&[4_usize, 6]).unwrap()
        );
    }

    #[test]
    fn test_zero_dim() {
        let buf = Buffer::from(&[1]);
        let tensor = UInt8Tensor::try_new(buf, None, None, None).unwrap();
        assert_eq!(0, tensor.size());
        assert_eq!(None, tensor.shape());
        assert_eq!(None, tensor.names());
        assert_eq!(0, tensor.ndim());
        assert_eq!(false, tensor.is_row_major().unwrap());
        assert_eq!(false, tensor.is_column_major().unwrap());
        assert_eq!(false, tensor.is_contiguous().unwrap());

        let buf = Buffer::from(&[1, 2, 2, 2]);
        let tensor = Int32Tensor::try_new(buf, None, None, None).unwrap();
        assert_eq!(0, tensor.size());
        assert_eq!(None, tensor.shape());
        assert_eq!(None, tensor.names());
        assert_eq!(0, tensor.ndim());
        assert_eq!(false, tensor.is_row_major().unwrap());
        assert_eq!(false, tensor.is_column_major().unwrap());
        assert_eq!(false, tensor.is_contiguous().unwrap());
    }

    #[test]
    fn test_tensor() {
        let mut builder = Int32BufferBuilder::new(16);
        for i in 0..16 {
            builder.append(i);
        }
        let buf = builder.finish();
        let tensor = Int32Tensor::try_new(buf, Some(vec![2, 8]), None, None).unwrap();
        assert_eq!(16, tensor.size());
        assert_eq!(Some(vec![2_usize, 8]).as_ref(), tensor.shape());
        assert_eq!(Some(vec![32_usize, 4]).as_ref(), tensor.strides());
        assert_eq!(2, tensor.ndim());
        assert_eq!(None, tensor.names());
    }

    #[test]
    fn test_new_row_major() {
        let mut builder = Int32BufferBuilder::new(16);
        for i in 0..16 {
            builder.append(i);
        }
        let buf = builder.finish();
        let tensor = Int32Tensor::new_row_major(buf, Some(vec![2, 8]), None).unwrap();
        assert_eq!(16, tensor.size());
        assert_eq!(Some(vec![2_usize, 8]).as_ref(), tensor.shape());
        assert_eq!(Some(vec![32_usize, 4]).as_ref(), tensor.strides());
        assert_eq!(None, tensor.names());
        assert_eq!(2, tensor.ndim());
        assert_eq!(true, tensor.is_row_major().unwrap());
        assert_eq!(false, tensor.is_column_major().unwrap());
        assert_eq!(true, tensor.is_contiguous().unwrap());
    }

    #[test]
    fn test_new_column_major() {
        let mut builder = Int32BufferBuilder::new(16);
        for i in 0..16 {
            builder.append(i);
        }
        let buf = builder.finish();
        let tensor = Int32Tensor::new_column_major(buf, Some(vec![2, 8]), None).unwrap();
        assert_eq!(16, tensor.size());
        assert_eq!(Some(vec![2_usize, 8]).as_ref(), tensor.shape());
        assert_eq!(Some(vec![4_usize, 8]).as_ref(), tensor.strides());
        assert_eq!(None, tensor.names());
        assert_eq!(2, tensor.ndim());
        assert_eq!(false, tensor.is_row_major().unwrap());
        assert_eq!(true, tensor.is_column_major().unwrap());
        assert_eq!(true, tensor.is_contiguous().unwrap());
    }

    #[test]
    fn test_with_names() {
        let mut builder = Int64BufferBuilder::new(8);
        for i in 0..8 {
            builder.append(i);
        }
        let buf = builder.finish();
        let names = vec!["Dim 1", "Dim 2"];
        let tensor =
            Int64Tensor::new_column_major(buf, Some(vec![2, 4]), Some(names)).unwrap();
        assert_eq!(8, tensor.size());
        assert_eq!(Some(vec![2_usize, 4]).as_ref(), tensor.shape());
        assert_eq!(Some(vec![8_usize, 16]).as_ref(), tensor.strides());
        assert_eq!("Dim 1", tensor.dim_name(0).unwrap());
        assert_eq!("Dim 2", tensor.dim_name(1).unwrap());
        assert_eq!(2, tensor.ndim());
        assert_eq!(false, tensor.is_row_major().unwrap());
        assert_eq!(true, tensor.is_column_major().unwrap());
        assert_eq!(true, tensor.is_contiguous().unwrap());
    }

    #[test]
    fn test_inconsistent_strides() {
        let mut builder = Int32BufferBuilder::new(16);
        for i in 0..16 {
            builder.append(i);
        }
        let buf = builder.finish();

        let result =
            Int32Tensor::try_new(buf, Some(vec![2, 8]), Some(vec![2, 8, 1]), None);

        if result.is_ok() {
            panic!("shape and stride dimensions are different")
        }
    }

    #[test]
    fn test_inconsistent_names() {
        let mut builder = Int32BufferBuilder::new(16);
        for i in 0..16 {
            builder.append(i);
        }
        let buf = builder.finish();

        let result = Int32Tensor::try_new(
            buf,
            Some(vec![2, 8]),
            Some(vec![4, 8]),
            Some(vec!["1", "2", "3"]),
        );

        if result.is_ok() {
            panic!("dimensions and names have different shape")
        }
    }

    #[test]
    fn test_incorrect_shape() {
        let mut builder = Int32BufferBuilder::new(16);
        for i in 0..16 {
            builder.append(i);
        }
        let buf = builder.finish();

        let result = Int32Tensor::try_new(buf, Some(vec![2, 6]), None, None);

        if result.is_ok() {
            panic!("number of elements does not match for the shape")
        }
    }

    #[test]
    fn test_incorrect_stride() {
        let mut builder = Int32BufferBuilder::new(16);
        for i in 0..16 {
            builder.append(i);
        }
        let buf = builder.finish();

        let result = Int32Tensor::try_new(buf, Some(vec![2, 8]), Some(vec![30, 4]), None);

        if result.is_ok() {
            panic!("the input stride does not match the selected shape")
        }
    }
}
