use std::marker::PhantomData;
use std::mem;


use datatypes::{DataType, ArrowPrimitiveType};
use buffer::Buffer;


fn compute_row_major_strides<T>(shape: &Vec<i64>) -> Vec<i64>
where T: ArrowPrimitiveType
{
    let mut remaining_bytes = mem::size_of::<T>() as i64;
    for i in shape {
        remaining_bytes *= i;
    }

    let mut strides = Vec::<i64>::new();
    for i in shape {
        remaining_bytes /= i;
        strides.push(remaining_bytes);
    }
    strides
}

fn compute_column_major_strides<T>(shape: &Vec<i64>) -> Vec<i64>
    where T: ArrowPrimitiveType
{
    let mut remaining_bytes = mem::size_of::<T>() as i64;
    let mut strides = Vec::<i64>::new();
    for i in shape {
        strides.push(remaining_bytes);
        remaining_bytes *= i;
    }
    strides
}


pub struct Tensor<T> where T: ArrowPrimitiveType {
    data_type: DataType,
    buffer: Buffer,
    shape: Option<Vec<i64>>,
    strides: Option<Vec<i64>>,
    names: Option<Vec<String>>,
    _marker: PhantomData<T>,
}

macro_rules! impl_tensor {
($data_ty:path, $native_ty:ident) => {
        impl Tensor<$native_ty> {
            pub fn new(buffer: Buffer,
                       shape: Option<Vec<i64>>,
                       strides: Option<Vec<i64>>,
                       names: Option<Vec<String>>) -> Self {
                if shape.is_none() {
                    assert_eq!(buffer.len(), mem::size_of::<$native_ty>(), "UPDATE");
                    assert_eq!(None, strides);
                    assert_eq!(None, names);
                }
                Self {
                    data_type: $data_ty,
                    buffer,
                    shape,
                    strides,
                    names,
                    _marker: PhantomData,
                }
            }

            pub fn new_row_major(buffer: Buffer,
                       shape: Option<Vec<i64>>,
                       names: Option<Vec<String>>) -> Self {
                let strides = match &shape {
                    None => None,
                    Some(ref s) => Some(compute_row_major_strides::<$native_ty>(&s))
                };
                Self::new(buffer, shape, strides, names)
            }

            pub fn new_column_major(buffer: Buffer,
                       shape: Option<Vec<i64>>,
                       names: Option<Vec<String>>) -> Self {
                let strides = match &shape {
                    None => None,
                    Some(ref s) => Some(compute_column_major_strides::<$native_ty>(&s))
                };
                Self::new(buffer, shape, strides, names)
            }

            pub fn data_type(&self) -> &DataType {
                &self.data_type
            }

            pub fn shape(&self) -> &Option<Vec<i64>> {
                &self.shape
            }

            pub fn strides(&self) -> Option<&Vec<i64>> {
                self.strides.as_ref()
            }

            pub fn names(&self) -> Option<&Vec<String>> {
                self.names.as_ref()
            }

            pub fn ndim(&self) -> i64 {
                2
            }

            pub fn size(&self) -> i64 {
                (self.buffer.len() / mem::size_of::<$native_ty>()) as i64
            }

            pub fn is_contiguous(&self) -> bool {
                true
            }

            pub fn is_row_major(&self) -> bool {
                true
            }

            pub fn is_column_major(&self) -> bool {
                true
            }
        }
    };
}

impl_tensor!(DataType::Boolean, bool);
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
        assert_eq!(vec![48, 8], compute_row_major_strides::<i64>(&vec![4_i64, 6]))
    }

    #[test]
    fn test_compute_column_major_strides() {
        assert_eq!(vec![8, 32], compute_column_major_strides::<i64>(&vec![4_i64, 6]))
    }

    #[test]
    fn test_zero_dim() {
        let buf = Buffer::from(&[1]);
        let tensor = Tensor::<u8>::new(buf, None, None, None);
        assert_eq!(1, tensor.size());
        assert_eq!(None, *tensor.shape());

        let buf = Buffer::from(&[1, 2, 2, 2]);
        let tensor = Tensor::<i32>::new(buf, None, None, None);
        assert_eq!(1, tensor.size());
        assert_eq!(None, *tensor.shape());
    }

    #[test]
    fn test_names() {
        let mut builder = BufferBuilder::<i32>::new(16);
        for i in 0..16 {
            builder.push(i).unwrap();
        }
        let buf = builder.finish();
        let tensor = Tensor::<i32>::new(buf, Some(vec![2, 8]), None, None);
        assert_eq!(16, tensor.size());
        assert_eq!(Some(vec![2_i64, 8]), *tensor.shape());
    }
}

