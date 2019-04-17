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

//! Arrow File Reader

use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::array::ArrayRef;
use crate::array_data::ArrayData;
use crate::buffer::Buffer;
use crate::datatypes::{DataType, Schema};
use crate::error::{ArrowError, Result};
use crate::ipc;
use crate::record_batch::RecordBatch;

static ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];

fn as_u32_le(array: &[u8; 4]) -> u32 {
    ((array[0] as u32) << 0)
        + ((array[1] as u32) << 8)
        + ((array[2] as u32) << 16)
        + ((array[3] as u32) << 24)
}

fn create_record_batch(schema: Schema, node: &ipc::FieldNode) -> Result<RecordBatch> {
    unimplemented!()
}

fn read_buffer(c_buf: &ipc::Buffer, a_data: &Vec<u8>) -> Buffer {
    let start_offset = c_buf.offset() as usize;
    let end_offset = start_offset + c_buf.length() as usize;
    let buf_data = &a_data[start_offset..end_offset];
    Buffer::from(&buf_data)
}

/// Reads the correct number of buffers based on data type and null_count, and creates an array ref
fn create_array(
    c_node: &ipc::FieldNode,
    data_type: &DataType,
    a_data: &Vec<u8>,
    c_bufs: &[ipc::Buffer],
    mut offset: usize,
) -> (ArrayRef, usize) {
    use DataType::*;
    let null_count = c_node.null_count() as usize;
    let array_data = match data_type {
        Utf8 => {
            if null_count > 0 {
                // read 3 buffers
                let array_data = ArrayData::new(
                    data_type.clone(),
                    c_node.length() as usize,
                    Some(null_count),
                    Some(read_buffer(&c_bufs[offset], a_data)),
                    0,
                    vec![
                        read_buffer(&c_bufs[offset + 1], a_data),
                        read_buffer(&c_bufs[offset + 2], a_data),
                    ],
                    vec![],
                );
                offset = offset + 3;
                array_data
            } else {
                // read 2 buffers
                let array_data = ArrayData::new(
                    data_type.clone(),
                    c_node.length() as usize,
                    Some(null_count),
                    None,
                    0,
                    vec![
                        read_buffer(&c_bufs[offset], a_data),
                        read_buffer(&c_bufs[offset + 1], a_data),
                    ],
                    vec![],
                );
                offset = offset + 2;
                array_data
            }
        }
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32
        | Boolean | Float64 => {
            if null_count > 0 {
                // read 3 buffers
                let array_data = ArrayData::new(
                    data_type.clone(),
                    c_node.length() as usize,
                    Some(null_count),
                    Some(read_buffer(&c_bufs[offset], a_data)),
                    0,
                    vec![read_buffer(&c_bufs[offset + 1], a_data)],
                    vec![],
                );
                offset = offset + 2;
                array_data
            } else {
                // read 2 buffers
                let array_data = ArrayData::new(
                    data_type.clone(),
                    c_node.length() as usize,
                    Some(null_count),
                    None,
                    0,
                    vec![read_buffer(&c_bufs[offset], a_data)],
                    vec![],
                );
                offset = offset + 1;
                array_data
            }
        }
        t @ _ => panic!("Data type {:?} not supported", t),
    };

    (crate::array::make_array(Arc::new(array_data)), offset)
}

pub struct Reader<R: Read + Seek> {
    reader: BufReader<R>,
    offset: usize,
    schema: Schema,
    blocks: Vec<ipc::Block>,
    current_block: usize,
    total_blocks: usize,
}

impl<R: Read + Seek> Reader<R> {
    /// create a new reader
    pub fn try_new(reader: R) -> Result<Self> {
        let mut reader = BufReader::new(reader);
        // check if header and footer contain correct magic bytes
        let mut magic_buffer: [u8; 6] = [0; 6];
        reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != ARROW_MAGIC {
            return Err(ArrowError::IoError(
                "Arrow file does not contain correct header".to_string(),
            ));
        }
        reader.seek(SeekFrom::End(-6))?;
        reader.read_exact(&mut magic_buffer)?;
        if magic_buffer != ARROW_MAGIC {
            return Err(ArrowError::IoError(
                "Arrow file does not contain correct footer".to_string(),
            ));
        }
        reader.seek(SeekFrom::Start(8))?;
        // determine metadata length
        let mut meta_size: [u8; 4] = [0; 4];
        reader.read_exact(&mut meta_size)?;
        let meta_len = as_u32_le(&meta_size);

        let mut meta_buffer = vec![0; meta_len as usize];
        reader.seek(SeekFrom::Start(12))?;
        reader.read_exact(&mut meta_buffer)?;

        let vecs = &meta_buffer.to_vec();
        let c_message = ipc::get_root_as_message(vecs);
        // message header is a Schema, so read it
        let c_schema: ipc::Schema = c_message.header_as_schema().unwrap();
        let schema = ipc::convert::fb_to_schema(c_schema);

        // what does the footer contain?
        let mut footer_size: [u8; 4] = [0; 4];
        reader.seek(SeekFrom::End(-10))?;
        reader.read_exact(&mut footer_size)?;
        let footer_len = as_u32_le(&footer_size);

        // read footer
        let mut footer_data = vec![0; footer_len as usize];
        reader.seek(SeekFrom::End(-10 - footer_len as i64))?;
        reader.read_exact(&mut footer_data)?;
        let c_footer = ipc::get_root_as_footer(&footer_data[..]);

        let c_blocks = c_footer.recordBatches().unwrap();

        dbg!(c_footer.recordBatches());
        let total_blocks = c_blocks.len();

        Ok(Self {
            reader,
            offset: 8 + 4 + meta_len as usize,
            schema,
            blocks: c_blocks.to_vec(),
            current_block: 0,
            total_blocks,
        })
    }

    /// Read file into record batches
    pub fn read(&mut self) -> Result<Option<RecordBatch>> {
        // get current block
        if self.current_block < self.total_blocks {
            let block = self.blocks[self.current_block];
            self.current_block = self.current_block + 1;

            // read length from end of offset
            let meta_len = block.metaDataLength() - 4;

            let mut block_data = vec![0; meta_len as usize];
            self.reader
                .seek(SeekFrom::Start(block.offset() as u64 + 4))?;
            self.reader.read_exact(&mut block_data)?;

            let c_block = ipc::get_root_as_message(&block_data[..]);

            match c_block.header_type() {
                ipc::MessageHeader::Schema => {
                    panic!("Not expecting a schema when messages are read")
                }
                ipc::MessageHeader::DictionaryBatch => {
                    unimplemented!("reading dictionary batches not yet supported")
                }
                ipc::MessageHeader::RecordBatch => {
                    let c_batch = c_block.header_as_record_batch().unwrap();
                    // read array data
                    let mut a_data = vec![0; block.bodyLength() as usize];
                    self.reader.seek(SeekFrom::Start(
                        block.offset() as u64 + block.metaDataLength() as u64,
                    ))?;
                    self.reader.read_exact(&mut a_data)?;

                    // construct buffers from their blocks
                    let c_buffers = c_batch.buffers().unwrap();

                    // get fields and determine number of buffers to use for each
                    let c_nodes = c_batch.nodes().unwrap();
                    let mut buffer_num = 0;
                    let mut field_num = 0;
                    let mut arrays = vec![];
                    for c_node in c_nodes {
                        let field = self.schema.field(field_num);
                        let (array, buffer) = create_array(
                            c_node,
                            field.data_type(),
                            &a_data,
                            c_buffers,
                            buffer_num,
                        );
                        field_num = field_num + 1;
                        buffer_num = buffer;

                        arrays.push(array);
                    }

                    RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)
                        .map(|batch| Some(batch))
                }
                ipc::MessageHeader::SparseTensor => panic!(),
                ipc::MessageHeader::Tensor => panic!("Can't be Tensor"),
                ipc::MessageHeader::NONE => panic!("Can't be NONE"),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    #[test]
    fn test_read_file() {
        let file = File::open("./test/data/arrow_file.dat").unwrap();

        let mut reader = Reader::try_new(file).unwrap();
        let batch: RecordBatch = reader.read().unwrap().unwrap();

        assert_eq!(5, batch.num_rows());
        assert_eq!(4, batch.num_columns());
        let arr_1 = batch.column(0);
    }
}
