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

//! Arrow IPC File and Stream Writers
//!
//! The `FileWriter` and `StreamWriter` have similar interfaces,
//! however the `FileWriter` expects a reader that supports `Seek`ing

use std::collections::HashMap;
use std::io::{BufWriter, Write};

use flatbuffers::FlatBufferBuilder;

use crate::array::{ArrayDataRef, ArrayRef};
use crate::buffer::{Buffer, MutableBuffer};
use crate::datatypes::*;
use crate::error::{ArrowError, Result};
use crate::ipc;
use crate::record_batch::RecordBatch;
use crate::util::bit_util;

use ipc::CONTINUATION_MARKER;

/// IPC write options used to control the behaviour of the writer
#[derive(Debug)]
pub struct IpcWriteOptions {
    /// Write padding after memory buffers to this multiple of bytes.
    /// Generally 8 or 64, defaults to 8
    alignment: usize,
    /// The legacy format is for releases before 0.15.0, and uses metadata V4
    write_legacy_ipc_format: bool,
    /// The metadata version to write. The Rust IPC writer supports V4+
    ///
    /// *Default versions per crate*
    ///
    /// When creating the default IpcWriteOptions, the following metadata versions are used:
    ///
    /// version 2.0.0: V4, with legacy format enabled
    /// version 4.0.0: V5
    metadata_version: ipc::MetadataVersion,
}

impl IpcWriteOptions {
    /// Try create IpcWriteOptions, checking for incompatible settings
    pub fn try_new(
        alignment: usize,
        write_legacy_ipc_format: bool,
        metadata_version: ipc::MetadataVersion,
    ) -> Result<Self> {
        if alignment == 0 || alignment % 8 != 0 {
            return Err(ArrowError::InvalidArgumentError(
                "Alignment should be greater than 0 and be a multiple of 8".to_string(),
            ));
        }
        match metadata_version {
            ipc::MetadataVersion::V1
            | ipc::MetadataVersion::V2
            | ipc::MetadataVersion::V3 => Err(ArrowError::InvalidArgumentError(
                "Writing IPC metadata version 3 and lower not supported".to_string(),
            )),
            ipc::MetadataVersion::V4 => Ok(Self {
                alignment,
                write_legacy_ipc_format,
                metadata_version,
            }),
            ipc::MetadataVersion::V5 => {
                if write_legacy_ipc_format {
                    Err(ArrowError::InvalidArgumentError(
                        "Legacy IPC format only supported on metadata version 4"
                            .to_string(),
                    ))
                } else {
                    Ok(Self {
                        alignment,
                        write_legacy_ipc_format,
                        metadata_version,
                    })
                }
            }
            z => panic!("Unsupported ipc::MetadataVersion {:?}", z),
        }
    }
}

impl Default for IpcWriteOptions {
    fn default() -> Self {
        Self {
            alignment: 8,
            write_legacy_ipc_format: false,
            metadata_version: ipc::MetadataVersion::V5,
        }
    }
}

#[derive(Debug, Default)]
pub struct IpcDataGenerator {}

impl IpcDataGenerator {
    pub fn schema_to_bytes(
        &self,
        schema: &Schema,
        write_options: &IpcWriteOptions,
    ) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();
        let schema = {
            let fb = ipc::convert::schema_to_fb_offset(&mut fbb, schema);
            fb.as_union_value()
        };

        let mut message = ipc::MessageBuilder::new(&mut fbb);
        message.add_version(write_options.metadata_version);
        message.add_header_type(ipc::MessageHeader::Schema);
        message.add_bodyLength(0);
        message.add_header(schema);
        // TODO: custom metadata
        let data = message.finish();
        fbb.finish(data, None);

        let data = fbb.finished_data();
        EncodedData {
            ipc_message: data.to_vec(),
            arrow_data: vec![],
        }
    }

    pub fn encoded_batch(
        &self,
        batch: &RecordBatch,
        dictionary_tracker: &mut DictionaryTracker,
        write_options: &IpcWriteOptions,
    ) -> Result<(Vec<EncodedData>, EncodedData)> {
        // TODO: handle nested dictionaries
        let schema = batch.schema();
        let mut encoded_dictionaries = Vec::with_capacity(schema.fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            let column = batch.column(i);

            if let DataType::Dictionary(_key_type, _value_type) = column.data_type() {
                let dict_id = field
                    .dict_id()
                    .expect("All Dictionary types have `dict_id`");
                let dict_data = column.data();
                let dict_values = &dict_data.child_data()[0];

                let emit = dictionary_tracker.insert(dict_id, column)?;

                if emit {
                    encoded_dictionaries.push(self.dictionary_batch_to_bytes(
                        dict_id,
                        dict_values,
                        write_options,
                    ));
                }
            }
        }

        let encoded_message = self.record_batch_to_bytes(batch, write_options);

        Ok((encoded_dictionaries, encoded_message))
    }

    /// Write a `RecordBatch` into two sets of bytes, one for the header (ipc::Message) and the
    /// other for the batch's data
    fn record_batch_to_bytes(
        &self,
        batch: &RecordBatch,
        write_options: &IpcWriteOptions,
    ) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();

        let mut nodes: Vec<ipc::FieldNode> = vec![];
        let mut buffers: Vec<ipc::Buffer> = vec![];
        let mut arrow_data: Vec<u8> = vec![];
        let mut offset = 0;
        for array in batch.columns() {
            let array_data = array.data();
            offset = write_array_data(
                &array_data,
                &mut buffers,
                &mut arrow_data,
                &mut nodes,
                offset,
                array.len(),
                array.null_count(),
            );
        }

        // write data
        let buffers = fbb.create_vector(&buffers);
        let nodes = fbb.create_vector(&nodes);

        let root = {
            let mut batch_builder = ipc::RecordBatchBuilder::new(&mut fbb);
            batch_builder.add_length(batch.num_rows() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            let b = batch_builder.finish();
            b.as_union_value()
        };
        // create an ipc::Message
        let mut message = ipc::MessageBuilder::new(&mut fbb);
        message.add_version(write_options.metadata_version);
        message.add_header_type(ipc::MessageHeader::RecordBatch);
        message.add_bodyLength(arrow_data.len() as i64);
        message.add_header(root);
        let root = message.finish();
        fbb.finish(root, None);
        let finished_data = fbb.finished_data();

        EncodedData {
            ipc_message: finished_data.to_vec(),
            arrow_data,
        }
    }

    /// Write dictionary values into two sets of bytes, one for the header (ipc::Message) and the
    /// other for the data
    fn dictionary_batch_to_bytes(
        &self,
        dict_id: i64,
        array_data: &ArrayDataRef,
        write_options: &IpcWriteOptions,
    ) -> EncodedData {
        let mut fbb = FlatBufferBuilder::new();

        let mut nodes: Vec<ipc::FieldNode> = vec![];
        let mut buffers: Vec<ipc::Buffer> = vec![];
        let mut arrow_data: Vec<u8> = vec![];

        write_array_data(
            &array_data,
            &mut buffers,
            &mut arrow_data,
            &mut nodes,
            0,
            array_data.len(),
            array_data.null_count(),
        );

        // write data
        let buffers = fbb.create_vector(&buffers);
        let nodes = fbb.create_vector(&nodes);

        let root = {
            let mut batch_builder = ipc::RecordBatchBuilder::new(&mut fbb);
            batch_builder.add_length(array_data.len() as i64);
            batch_builder.add_nodes(nodes);
            batch_builder.add_buffers(buffers);
            batch_builder.finish()
        };

        let root = {
            let mut batch_builder = ipc::DictionaryBatchBuilder::new(&mut fbb);
            batch_builder.add_id(dict_id);
            batch_builder.add_data(root);
            batch_builder.finish().as_union_value()
        };

        let root = {
            let mut message_builder = ipc::MessageBuilder::new(&mut fbb);
            message_builder.add_version(write_options.metadata_version);
            message_builder.add_header_type(ipc::MessageHeader::DictionaryBatch);
            message_builder.add_bodyLength(arrow_data.len() as i64);
            message_builder.add_header(root);
            message_builder.finish()
        };

        fbb.finish(root, None);
        let finished_data = fbb.finished_data();

        EncodedData {
            ipc_message: finished_data.to_vec(),
            arrow_data,
        }
    }
}

/// Keeps track of dictionaries that have been written, to avoid emitting the same dictionary
/// multiple times. Can optionally error if an update to an existing dictionary is attempted, which
/// isn't allowed in the `FileWriter`.
pub struct DictionaryTracker {
    written: HashMap<i64, ArrayRef>,
    error_on_replacement: bool,
}

impl DictionaryTracker {
    pub fn new(error_on_replacement: bool) -> Self {
        Self {
            written: HashMap::new(),
            error_on_replacement,
        }
    }

    /// Keep track of the dictionary with the given ID and values. Behavior:
    ///
    /// * If this ID has been written already and has the same data, return `Ok(false)` to indicate
    ///   that the dictionary was not actually inserted (because it's already been seen).
    /// * If this ID has been written already but with different data, and this tracker is
    ///   configured to return an error, return an error.
    /// * If the tracker has not been configured to error on replacement or this dictionary
    ///   has never been seen before, return `Ok(true)` to indicate that the dictionary was just
    ///   inserted.
    pub fn insert(&mut self, dict_id: i64, column: &ArrayRef) -> Result<bool> {
        let dict_data = column.data();
        let dict_values = &dict_data.child_data()[0];

        // If a dictionary with this id was already emitted, check if it was the same.
        if let Some(last) = self.written.get(&dict_id) {
            if last.data().child_data()[0] == *dict_values {
                // Same dictionary values => no need to emit it again
                return Ok(false);
            } else if self.error_on_replacement {
                return Err(ArrowError::InvalidArgumentError(
                    "Dictionary replacement detected when writing IPC file format. \
                     Arrow IPC files only support a single dictionary for a given field \
                     across all batches."
                        .to_string(),
                ));
            }
        }

        self.written.insert(dict_id, column.clone());
        Ok(true)
    }
}

pub struct FileWriter<W: Write> {
    /// The object to write to
    writer: BufWriter<W>,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: Schema,
    /// The number of bytes between each block of bytes, as an offset for random access
    block_offsets: usize,
    /// Dictionary blocks that will be written as part of the IPC footer
    dictionary_blocks: Vec<ipc::Block>,
    /// Record blocks that will be written as part of the IPC footer
    record_blocks: Vec<ipc::Block>,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,
}

impl<W: Write> FileWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    /// Try create a new writer with IpcWriteOptions
    pub fn try_new_with_options(
        writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let data_gen = IpcDataGenerator::default();
        let mut writer = BufWriter::new(writer);
        // write magic to header
        writer.write_all(&super::ARROW_MAGIC[..])?;
        // create an 8-byte boundary after the header
        writer.write_all(&[0, 0])?;
        // write the schema, set the written bytes to the schema + header
        let encoded_message = data_gen.schema_to_bytes(schema, &write_options);
        let (meta, data) = write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            schema: schema.clone(),
            block_offsets: meta + data + 8,
            dictionary_blocks: vec![],
            record_blocks: vec![],
            finished: false,
            dictionary_tracker: DictionaryTracker::new(true),
            data_gen,
        })
    }

    /// Write a record batch to the file
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write record batch to file writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self.data_gen.encoded_batch(
            batch,
            &mut self.dictionary_tracker,
            &self.write_options,
        )?;

        for encoded_dictionary in encoded_dictionaries {
            let (meta, data) =
                write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;

            let block =
                ipc::Block::new(self.block_offsets as i64, meta as i32, data as i64);
            self.dictionary_blocks.push(block);
            self.block_offsets += meta + data;
        }

        let (meta, data) =
            write_message(&mut self.writer, encoded_message, &self.write_options)?;
        // add a record block for the footer
        let block = ipc::Block::new(
            self.block_offsets as i64,
            meta as i32, // TODO: is this still applicable?
            data as i64,
        );
        self.record_blocks.push(block);
        self.block_offsets += meta + data;
        Ok(())
    }

    /// Write footer and closing tag, then mark the writer as done
    pub fn finish(&mut self) -> Result<()> {
        // write EOS
        write_continuation(&mut self.writer, &self.write_options, 0)?;

        let mut fbb = FlatBufferBuilder::new();
        let dictionaries = fbb.create_vector(&self.dictionary_blocks);
        let record_batches = fbb.create_vector(&self.record_blocks);
        let schema = ipc::convert::schema_to_fb_offset(&mut fbb, &self.schema);

        let root = {
            let mut footer_builder = ipc::FooterBuilder::new(&mut fbb);
            footer_builder.add_version(self.write_options.metadata_version);
            footer_builder.add_schema(schema);
            footer_builder.add_dictionaries(dictionaries);
            footer_builder.add_recordBatches(record_batches);
            footer_builder.finish()
        };
        fbb.finish(root, None);
        let footer_data = fbb.finished_data();
        self.writer.write_all(footer_data)?;
        self.writer
            .write_all(&(footer_data.len() as i32).to_le_bytes())?;
        self.writer.write_all(&super::ARROW_MAGIC)?;
        self.writer.flush()?;
        self.finished = true;

        Ok(())
    }
}

/// Finish the file if it is not 'finished' when it goes out of scope
impl<W: Write> Drop for FileWriter<W> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish().unwrap();
        }
    }
}

pub struct StreamWriter<W: Write> {
    /// The object to write to
    writer: BufWriter<W>,
    /// IPC write options
    write_options: IpcWriteOptions,
    /// A reference to the schema, used in validating record batches
    schema: Schema,
    /// Whether the writer footer has been written, and the writer is finished
    finished: bool,
    /// Keeps track of dictionaries that have been written
    dictionary_tracker: DictionaryTracker,

    data_gen: IpcDataGenerator,
}

impl<W: Write> StreamWriter<W> {
    /// Try create a new writer, with the schema written as part of the header
    pub fn try_new(writer: W, schema: &Schema) -> Result<Self> {
        let write_options = IpcWriteOptions::default();
        Self::try_new_with_options(writer, schema, write_options)
    }

    pub fn try_new_with_options(
        writer: W,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let data_gen = IpcDataGenerator::default();
        let mut writer = BufWriter::new(writer);
        // write the schema, set the written bytes to the schema
        let encoded_message = data_gen.schema_to_bytes(schema, &write_options);
        write_message(&mut writer, encoded_message, &write_options)?;
        Ok(Self {
            writer,
            write_options,
            schema: schema.clone(),
            finished: false,
            dictionary_tracker: DictionaryTracker::new(false),
            data_gen,
        })
    }

    /// Write a record batch to the stream
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.finished {
            return Err(ArrowError::IoError(
                "Cannot write record batch to stream writer as it is closed".to_string(),
            ));
        }

        let (encoded_dictionaries, encoded_message) = self
            .data_gen
            .encoded_batch(batch, &mut self.dictionary_tracker, &self.write_options)
            .expect("StreamWriter is configured to not error on dictionary replacement");

        for encoded_dictionary in encoded_dictionaries {
            write_message(&mut self.writer, encoded_dictionary, &self.write_options)?;
        }

        write_message(&mut self.writer, encoded_message, &self.write_options)?;
        Ok(())
    }

    /// Write continuation bytes, and mark the stream as done
    pub fn finish(&mut self) -> Result<()> {
        write_continuation(&mut self.writer, &self.write_options, 0)?;

        self.finished = true;

        Ok(())
    }
}

/// Finish the stream if it is not 'finished' when it goes out of scope
impl<W: Write> Drop for StreamWriter<W> {
    fn drop(&mut self) {
        if !self.finished {
            self.finish().unwrap();
        }
    }
}

/// Stores the encoded data, which is an ipc::Message, and optional Arrow data
pub struct EncodedData {
    /// An encoded ipc::Message
    pub ipc_message: Vec<u8>,
    /// Arrow buffers to be written, should be an empty vec for schema messages
    pub arrow_data: Vec<u8>,
}
/// Write a message's IPC data and buffers, returning metadata and buffer data lengths written
pub fn write_message<W: Write>(
    mut writer: W,
    encoded: EncodedData,
    write_options: &IpcWriteOptions,
) -> Result<(usize, usize)> {
    let arrow_data_len = encoded.arrow_data.len();
    if arrow_data_len % 8 != 0 {
        return Err(ArrowError::MemoryError(
            "Arrow data not aligned".to_string(),
        ));
    }

    let a = write_options.alignment - 1;
    let buffer = encoded.ipc_message;
    let flatbuf_size = buffer.len();
    let prefix_size = if write_options.write_legacy_ipc_format {
        4
    } else {
        8
    };
    let aligned_size = (flatbuf_size + prefix_size + a) & !a;
    let padding_bytes = aligned_size - flatbuf_size - prefix_size;

    write_continuation(
        &mut writer,
        &write_options,
        (aligned_size - prefix_size) as i32,
    )?;

    // write the flatbuf
    if flatbuf_size > 0 {
        writer.write_all(&buffer)?;
    }
    // write padding
    writer.write_all(&vec![0; padding_bytes])?;

    // write arrow data
    let body_len = if arrow_data_len > 0 {
        write_body_buffers(&mut writer, &encoded.arrow_data)?
    } else {
        0
    };

    Ok((aligned_size, body_len))
}

fn write_body_buffers<W: Write>(mut writer: W, data: &[u8]) -> Result<usize> {
    let len = data.len() as u32;
    let pad_len = pad_to_8(len) as u32;
    let total_len = len + pad_len;

    // write body buffer
    writer.write_all(data)?;
    if pad_len > 0 {
        writer.write_all(&vec![0u8; pad_len as usize][..])?;
    }

    writer.flush()?;
    Ok(total_len as usize)
}

/// Write a record batch to the writer, writing the message size before the message
/// if the record batch is being written to a stream
fn write_continuation<W: Write>(
    mut writer: W,
    write_options: &IpcWriteOptions,
    total_len: i32,
) -> Result<usize> {
    let mut written = 8;

    // the version of the writer determines whether continuation markers should be added
    match write_options.metadata_version {
        ipc::MetadataVersion::V1
        | ipc::MetadataVersion::V2
        | ipc::MetadataVersion::V3 => {
            unreachable!("Options with the metadata version cannot be created")
        }
        ipc::MetadataVersion::V4 => {
            if !write_options.write_legacy_ipc_format {
                // v0.15.0 format
                writer.write_all(&CONTINUATION_MARKER)?;
                written = 4;
            }
            writer.write_all(&total_len.to_le_bytes()[..])?;
        }
        ipc::MetadataVersion::V5 => {
            // write continuation marker and message length
            writer.write_all(&CONTINUATION_MARKER)?;
            writer.write_all(&total_len.to_le_bytes()[..])?;
        }
        z => panic!("Unsupported ipc::MetadataVersion {:?}", z),
    };

    writer.flush()?;

    Ok(written)
}

/// Write array data to a vector of bytes
fn write_array_data(
    array_data: &ArrayDataRef,
    mut buffers: &mut Vec<ipc::Buffer>,
    mut arrow_data: &mut Vec<u8>,
    mut nodes: &mut Vec<ipc::FieldNode>,
    offset: i64,
    num_rows: usize,
    null_count: usize,
) -> i64 {
    let mut offset = offset;
    nodes.push(ipc::FieldNode::new(num_rows as i64, null_count as i64));
    // NullArray does not have any buffers, thus the null buffer is not generated
    if array_data.data_type() != &DataType::Null {
        // write null buffer if exists
        let null_buffer = match array_data.null_buffer() {
            None => {
                // create a buffer and fill it with valid bits
                let num_bytes = bit_util::ceil(num_rows, 8);
                let buffer = MutableBuffer::new(num_bytes);
                let buffer = buffer.with_bitset(num_bytes, true);
                buffer.into()
            }
            Some(buffer) => buffer.clone(),
        };

        offset = write_buffer(&null_buffer, &mut buffers, &mut arrow_data, offset);
    }

    array_data.buffers().iter().for_each(|buffer| {
        offset = write_buffer(buffer, &mut buffers, &mut arrow_data, offset);
    });

    if !matches!(array_data.data_type(), DataType::Dictionary(_, _)) {
        // recursively write out nested structures
        array_data.child_data().iter().for_each(|data_ref| {
            // write the nested data (e.g list data)
            offset = write_array_data(
                data_ref,
                &mut buffers,
                &mut arrow_data,
                &mut nodes,
                offset,
                data_ref.len(),
                data_ref.null_count(),
            );
        });
    }

    offset
}

/// Write a buffer to a vector of bytes, and add its ipc::Buffer to a vector
fn write_buffer(
    buffer: &Buffer,
    buffers: &mut Vec<ipc::Buffer>,
    arrow_data: &mut Vec<u8>,
    offset: i64,
) -> i64 {
    let len = buffer.len();
    let pad_len = pad_to_8(len as u32);
    let total_len: i64 = (len + pad_len) as i64;
    // assert_eq!(len % 8, 0, "Buffer width not a multiple of 8 bytes");
    buffers.push(ipc::Buffer::new(offset, total_len));
    arrow_data.extend_from_slice(buffer.as_slice());
    arrow_data.extend_from_slice(&vec![0u8; pad_len][..]);
    offset + total_len
}

/// Calculate an 8-byte boundary and return the number of bytes needed to pad to 8 bytes
#[inline]
fn pad_to_8(len: u32) -> usize {
    (((len + 7) & !7) - len) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::Read;
    use std::sync::Arc;

    use flate2::read::GzDecoder;
    use ipc::MetadataVersion;

    use crate::array::*;
    use crate::datatypes::Field;
    use crate::ipc::reader::*;
    use crate::util::integration_util::*;

    #[test]
    fn test_write_file() {
        let schema = Schema::new(vec![Field::new("field1", DataType::UInt32, false)]);
        let values: Vec<Option<u32>> = vec![
            Some(999),
            None,
            Some(235),
            Some(123),
            None,
            None,
            None,
            None,
            None,
        ];
        let array1 = UInt32Array::from(values);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(array1) as ArrayRef],
        )
        .unwrap();
        {
            let file = File::create("target/debug/testdata/arrow.arrow_file").unwrap();
            let mut writer = FileWriter::try_new(file, &schema).unwrap();

            writer.write(&batch).unwrap();
            // this is inside a block to test the implicit finishing of the file on `Drop`
        }

        {
            let file =
                File::open(format!("target/debug/testdata/{}.arrow_file", "arrow"))
                    .unwrap();
            let mut reader = FileReader::try_new(file).unwrap();
            while let Some(Ok(read_batch)) = reader.next() {
                read_batch
                    .columns()
                    .iter()
                    .zip(batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
            }
        }
    }

    fn write_null_file(options: IpcWriteOptions, suffix: &str) {
        let schema = Schema::new(vec![
            Field::new("nulls", DataType::Null, true),
            Field::new("int32s", DataType::Int32, false),
            Field::new("nulls2", DataType::Null, false),
            Field::new("f64s", DataType::Float64, false),
        ]);
        let array1 = NullArray::new(32);
        let array2 = Int32Array::from(vec![1; 32]);
        let array3 = NullArray::new(32);
        let array4 = Float64Array::from(vec![std::f64::NAN; 32]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(array1) as ArrayRef,
                Arc::new(array2) as ArrayRef,
                Arc::new(array3) as ArrayRef,
                Arc::new(array4) as ArrayRef,
            ],
        )
        .unwrap();
        let file_name = format!("target/debug/testdata/nulls_{}.arrow_file", suffix);
        {
            let file = File::create(&file_name).unwrap();
            let mut writer =
                FileWriter::try_new_with_options(file, &schema, options).unwrap();

            writer.write(&batch).unwrap();
            // this is inside a block to test the implicit finishing of the file on `Drop`
        }

        {
            let file = File::open(&file_name).unwrap();
            let reader = FileReader::try_new(file).unwrap();
            reader.for_each(|maybe_batch| {
                maybe_batch
                    .unwrap()
                    .columns()
                    .iter()
                    .zip(batch.columns())
                    .for_each(|(a, b)| {
                        assert_eq!(a.data_type(), b.data_type());
                        assert_eq!(a.len(), b.len());
                        assert_eq!(a.null_count(), b.null_count());
                    });
            });
        }
    }
    #[test]
    fn test_write_null_file_v4() {
        write_null_file(
            IpcWriteOptions::try_new(8, false, MetadataVersion::V4).unwrap(),
            "v4_a8",
        );
        write_null_file(
            IpcWriteOptions::try_new(8, true, MetadataVersion::V4).unwrap(),
            "v4_a8l",
        );
        write_null_file(
            IpcWriteOptions::try_new(64, false, MetadataVersion::V4).unwrap(),
            "v4_a64",
        );
        write_null_file(
            IpcWriteOptions::try_new(64, true, MetadataVersion::V4).unwrap(),
            "v4_a64l",
        );
    }

    #[test]
    fn test_write_null_file_v5() {
        write_null_file(
            IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap(),
            "v5_a8",
        );
        write_null_file(
            IpcWriteOptions::try_new(64, false, MetadataVersion::V5).unwrap(),
            "v5_a64",
        );
    }

    #[test]
    fn read_and_rewrite_generated_files_014() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "0.14.1";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_interval",
            "generated_datetime",
            "generated_dictionary",
            "generated_nested",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            "generated_decimal",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
                testdata, version, path
            ))
            .unwrap();

            let mut reader = FileReader::try_new(file).unwrap();

            // read and rewrite the file to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.arrow_file",
                    version, path
                ))
                .unwrap();
                let mut writer = FileWriter::try_new(file, &reader.schema()).unwrap();
                while let Some(Ok(batch)) = reader.next() {
                    writer.write(&batch).unwrap();
                }
                writer.finish().unwrap();
            }

            let file = File::open(format!(
                "target/debug/testdata/{}-{}.arrow_file",
                version, path
            ))
            .unwrap();
            let mut reader = FileReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    #[test]
    fn read_and_rewrite_generated_streams_014() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "0.14.1";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_interval",
            "generated_datetime",
            "generated_dictionary",
            "generated_nested",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            "generated_decimal",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.stream",
                testdata, version, path
            ))
            .unwrap();

            let reader = StreamReader::try_new(file).unwrap();

            // read and rewrite the stream to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.stream",
                    version, path
                ))
                .unwrap();
                let mut writer = StreamWriter::try_new(file, &reader.schema()).unwrap();
                reader.for_each(|batch| {
                    writer.write(&batch.unwrap()).unwrap();
                });
                writer.finish().unwrap();
            }

            let file =
                File::open(format!("target/debug/testdata/{}-{}.stream", version, path))
                    .unwrap();
            let mut reader = StreamReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    #[test]
    fn read_and_rewrite_generated_files_100() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "1.0.0-littleendian";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_custom_metadata",
            "generated_datetime",
            "generated_dictionary_unsigned",
            "generated_dictionary",
            // "generated_duplicate_fieldnames",
            "generated_interval",
            "generated_large_batch",
            "generated_nested",
            // "generated_nested_large_offsets",
            "generated_null_trivial",
            "generated_null",
            "generated_primitive_large_offsets",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            // "generated_recursive_nested",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.arrow_file",
                testdata, version, path
            ))
            .unwrap();

            let mut reader = FileReader::try_new(file).unwrap();

            // read and rewrite the file to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.arrow_file",
                    version, path
                ))
                .unwrap();
                // write IPC version 5
                let options =
                    IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
                let mut writer =
                    FileWriter::try_new_with_options(file, &reader.schema(), options)
                        .unwrap();
                while let Some(Ok(batch)) = reader.next() {
                    writer.write(&batch).unwrap();
                }
                writer.finish().unwrap();
            }

            let file = File::open(format!(
                "target/debug/testdata/{}-{}.arrow_file",
                version, path
            ))
            .unwrap();
            let mut reader = FileReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    #[test]
    fn read_and_rewrite_generated_streams_100() {
        let testdata = crate::util::test_util::arrow_test_data();
        let version = "1.0.0-littleendian";
        // the test is repetitive, thus we can read all supported files at once
        let paths = vec![
            "generated_custom_metadata",
            "generated_datetime",
            "generated_dictionary_unsigned",
            "generated_dictionary",
            // "generated_duplicate_fieldnames",
            "generated_interval",
            "generated_large_batch",
            "generated_nested",
            // "generated_nested_large_offsets",
            "generated_null_trivial",
            "generated_null",
            "generated_primitive_large_offsets",
            "generated_primitive_no_batches",
            "generated_primitive_zerolength",
            "generated_primitive",
            // "generated_recursive_nested",
        ];
        paths.iter().for_each(|path| {
            let file = File::open(format!(
                "{}/arrow-ipc-stream/integration/{}/{}.stream",
                testdata, version, path
            ))
            .unwrap();

            let reader = StreamReader::try_new(file).unwrap();

            // read and rewrite the stream to a temp location
            {
                let file = File::create(format!(
                    "target/debug/testdata/{}-{}.stream",
                    version, path
                ))
                .unwrap();
                let options =
                    IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
                let mut writer =
                    StreamWriter::try_new_with_options(file, &reader.schema(), options)
                        .unwrap();
                reader.for_each(|batch| {
                    writer.write(&batch.unwrap()).unwrap();
                });
                writer.finish().unwrap();
            }

            let file =
                File::open(format!("target/debug/testdata/{}-{}.stream", version, path))
                    .unwrap();
            let mut reader = StreamReader::try_new(file).unwrap();

            // read expected JSON output
            let arrow_json = read_gzip_json(version, path);
            assert!(arrow_json.equals_reader(&mut reader));
        });
    }

    /// Read gzipped JSON file
    fn read_gzip_json(version: &str, path: &str) -> ArrowJson {
        let testdata = crate::util::test_util::arrow_test_data();
        let file = File::open(format!(
            "{}/arrow-ipc-stream/integration/{}/{}.json.gz",
            testdata, version, path
        ))
        .unwrap();
        let mut gz = GzDecoder::new(&file);
        let mut s = String::new();
        gz.read_to_string(&mut s).unwrap();
        // convert to Arrow JSON
        let arrow_json: ArrowJson = serde_json::from_str(&s).unwrap();
        arrow_json
    }
}
