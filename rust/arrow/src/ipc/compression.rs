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

//! IPC Compression Utilities

use std::io::{Read, Write};

use crate::error::{ArrowError, Result};
use crate::ipc::gen::Message::CompressionType;

pub trait IpcCompressionCodec {
    fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()>;
    fn decompress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize>;
    fn get_compression_type(&self) -> CompressionType;
}

pub type Codec = Box<dyn IpcCompressionCodec>;

#[inline]
pub(crate) fn get_codec(
    compression_type: Option<CompressionType>,
) -> Result<Option<Codec>> {
    match compression_type {
        Some(CompressionType::LZ4_FRAME) => Ok(Some(Box::new(Lz4CompressionCodec {}))),
        Some(ctype) => Err(ArrowError::InvalidArgumentError(format!(
            "IPC CompresstionType {:?} not yet supported",
            ctype
        ))),
        None => Ok(None),
    }
}

pub struct Lz4CompressionCodec {}
const LZ4_BUFFER_SIZE: usize = 4096;

impl IpcCompressionCodec for Lz4CompressionCodec {
    fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
        if input_buf.is_empty() {
            output_buf.write_all(&8i64.to_le_bytes())?;
            return Ok(());
        }
        // write out the uncompressed length as a LE i64 value
        output_buf.write_all(&(input_buf.len() as i64).to_le_bytes())?;
        let mut encoder = lz4::EncoderBuilder::new().build(output_buf)?;
        let mut from = 0;
        loop {
            let to = std::cmp::min(from + LZ4_BUFFER_SIZE, input_buf.len());
            encoder.write_all(&input_buf[from..to])?;
            from += LZ4_BUFFER_SIZE;
            if from >= input_buf.len() {
                break;
            }
        }
        Ok(encoder.finish().1?)
    }

    fn decompress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
        if input_buf.is_empty() {
            return Ok(0);
        }
        let mut decoder = lz4::Decoder::new(&input_buf[8..])?;
        let mut buffer: [u8; LZ4_BUFFER_SIZE] = [0; LZ4_BUFFER_SIZE];
        let mut total_len = 0;
        loop {
            let len = decoder.read(&mut buffer)?;
            if len == 0 {
                break;
            }
            total_len += len;
            output_buf.write_all(&buffer[0..len])?;
        }
        decoder.finish().1?;
        Ok(total_len)
    }

    fn get_compression_type(&self) -> CompressionType {
        CompressionType::LZ4_FRAME
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::Rng;

    use crate::util::test_util::seedable_rng;

    const INPUT_BUFFER_LEN: usize = 256;

    #[test]
    fn test_lz4_roundtrip() {
        let mut rng = seedable_rng();
        let mut bytes: Vec<u8> = Vec::with_capacity(INPUT_BUFFER_LEN);

        (0..INPUT_BUFFER_LEN).for_each(|_| {
            bytes.push(rng.gen::<u8>());
        });

        let codec = Lz4CompressionCodec {};
        let mut compressed = Vec::new();
        codec.compress(&bytes, &mut compressed).unwrap();

        let mut decompressed = Vec::new();
        let _ = codec.decompress(&compressed, &mut decompressed).unwrap();
        assert_eq!(decompressed, bytes);
    }
}
