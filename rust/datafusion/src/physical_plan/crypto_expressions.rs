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

use md5::Md5;
use sha2::{
    digest::Output as SHA2DigestOutput, Digest as SHA2Digest, Sha224, Sha256, Sha384,
    Sha512,
};

use crate::error::{DataFusionError, Result};
use arrow::array::{
    ArrayRef, GenericBinaryArray, GenericStringArray, StringOffsetSizeTrait,
};

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

macro_rules! crypto_unary_string_function {
    ($NAME:ident, $FUNC:expr) => {
        /// crypto function that accepts Utf8 or LargeUtf8 and returns Utf8 string
        pub fn $NAME<T: StringOffsetSizeTrait>(
            args: &[ArrayRef],
        ) -> Result<GenericStringArray<i32>> {
            if args.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "{:?} args were supplied but {} takes exactly one argument",
                    args.len(),
                    String::from(stringify!($NAME)),
                )));
            }

            let array = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            // first map is the iterator, second is for the `Option<_>`
            Ok(array.iter().map(|x| x.map(|x| $FUNC(x))).collect())
        }
    };
}

macro_rules! crypto_unary_binary_function {
    ($NAME:ident, $FUNC:expr) => {
        /// crypto function that accepts Utf8 or LargeUtf8 and returns Binary
        pub fn $NAME<T: StringOffsetSizeTrait>(
            args: &[ArrayRef],
        ) -> Result<GenericBinaryArray<i32>> {
            if args.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "{:?} args were supplied but {} takes exactly one argument",
                    args.len(),
                    String::from(stringify!($NAME)),
                )));
            }

            let array = args[0]
                .as_any()
                .downcast_ref::<GenericStringArray<T>>()
                .unwrap();

            // first map is the iterator, second is for the `Option<_>`
            Ok(array.iter().map(|x| x.map(|x| $FUNC(x))).collect())
        }
    };
}

crypto_unary_string_function!(md5, md5_process);
crypto_unary_binary_function!(sha224, sha_process::<Sha224>);
crypto_unary_binary_function!(sha256, sha_process::<Sha256>);
crypto_unary_binary_function!(sha384, sha_process::<Sha384>);
crypto_unary_binary_function!(sha512, sha_process::<Sha512>);
