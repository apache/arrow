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

//! Crypto expressions
use std::sync::Arc;

use md5::Md5;
use sha2::{
    digest::Output as SHA2DigestOutput, Digest as SHA2Digest, Sha224, Sha256, Sha384,
    Sha512,
};

use crate::{
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use arrow::{
    array::{Array, BinaryArray, GenericStringArray, StringOffsetSizeTrait},
    datatypes::DataType,
};

use super::{string_expressions::unary_string_function, ColumnarValue};

/// Computes the md5 of a string.
fn md5_process(input: &str) -> String {
    let mut digest = Md5::default();
    digest.update(&input);

    let mut result = String::new();

    for byte in &digest.finalize() {
        result.push_str(&format!("{:02x}", byte));
    }

    result
}

// It's not possible to return &[u8], because trait in trait without short lifetime
fn sha_process<D: SHA2Digest + Default>(input: &str) -> SHA2DigestOutput<D> {
    let mut digest = D::default();
    digest.update(&input);

    digest.finalize()
}

/// # Errors
/// This function errors when:
/// * the number of arguments is not 1
/// * the first argument is not castable to a `GenericStringArray`
fn unary_binary_function<T, R, F>(
    args: &[&dyn Array],
    op: F,
    name: &str,
) -> Result<BinaryArray>
where
    R: AsRef<[u8]>,
    T: StringOffsetSizeTrait,
    F: Fn(&str) -> R,
{
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name,
        )));
    }

    let array = args[0]
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Internal("failed to downcast to string".to_string())
        })?;

    // first map is the iterator, second is for the `Option<_>`
    Ok(array.iter().map(|x| x.map(|x| op(x))).collect())
}

fn handle<F, R>(args: &[ColumnarValue], op: F, name: &str) -> Result<ColumnarValue>
where
    R: AsRef<[u8]>,
    F: Fn(&str) -> R,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_binary_function::<
                    i32,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            DataType::LargeUtf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_binary_function::<
                    i64,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name,
            ))),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_vec());
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_vec());
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(result)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function {}",
                other, name,
            ))),
        },
    }
}

fn md5_array<T: StringOffsetSizeTrait>(
    args: &[&dyn Array],
) -> Result<GenericStringArray<i32>> {
    unary_string_function::<T, i32, _, _>(args, md5_process, "md5")
}

/// crypto function that accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`]
pub fn md5(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(md5_array::<i32>(&[
                a.as_ref()
            ])?))),
            DataType::LargeUtf8 => {
                Ok(ColumnarValue::Array(Arc::new(md5_array::<i64>(&[
                    a.as_ref()
                ])?)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function md5",
                other,
            ))),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| md5_process(x));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| md5_process(x));
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function md5",
                other,
            ))),
        },
    }
}

/// crypto function that accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`]
pub fn sha224(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, sha_process::<Sha224>, "ssh224")
}

/// crypto function that accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`]
pub fn sha256(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, sha_process::<Sha256>, "sha256")
}

/// crypto function that accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`]
pub fn sha384(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, sha_process::<Sha384>, "sha384")
}

/// crypto function that accepts Utf8 or LargeUtf8 and returns a [`ColumnarValue`]
pub fn sha512(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle(args, sha_process::<Sha512>, "sha512")
}
