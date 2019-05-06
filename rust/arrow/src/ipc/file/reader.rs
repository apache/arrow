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

/// Read a buffer based on offset and length
fn read_buffer(buf: &ipc::Buffer, a_data: &Vec<u8>) -> Buffer {
    let start_offset = buf.offset() as usize;
    let end_offset = start_offset + buf.length() as usize;
    let buf_data = &a_data[start_offset..end_offset];
    Buffer::from(&buf_data)
}

/// Coordinates reading arrays based on data types
fn create_array(
    nodes: &[ipc::FieldNode],
    data_type: &DataType,
    data: &Vec<u8>,
    buffers: &[ipc::Buffer],
    mut node_index: usize,
    mut buffer_index: usize,
) -> (ArrayRef, usize, usize) {
    use DataType::*;
    let array = match data_type {
        Utf8 => {
            let array = create_primitive_array(
                &nodes[node_index],
                data_type,
                buffers[buffer_index..buffer_index + 3]
                    .iter()
                    .map(|buf| read_buffer(buf, data))
                    .collect(),
            );
            node_index = node_index + 1;
            buffer_index = buffer_index + 3;
            array
        }
        List(ref list_data_type) => {
            let list_node = &nodes[node_index];
            let list_buffers: Vec<Buffer> = buffers[buffer_index..buffer_index + 2]
                .iter()
                .map(|buf| read_buffer(buf, data))
                .collect();
            node_index = node_index + 1;
            buffer_index = buffer_index + 2;
            let triple = create_array(
                nodes,
                list_data_type,
                data,
                buffers,
                node_index,
                buffer_index,
            );
            node_index = triple.1;
            buffer_index = triple.2;

            create_list_array(list_node, data_type, &list_buffers[..], triple.0)
        }
        Struct(struct_fields) => {
            let struct_node = &nodes[node_index];
            let null_buffer: Buffer = read_buffer(&buffers[buffer_index], data);
            node_index = node_index + 1;
            buffer_index = buffer_index + 1;

            // read the arrays for each field
            let mut struct_arrays = vec![];
            // TODO investigate whether just knowing the number of buffers could
            // still work
            for struct_field in struct_fields {
                let triple = create_array(
                    nodes,
                    struct_field.data_type(),
                    data,
                    buffers,
                    node_index,
                    buffer_index,
                );
                node_index = triple.1;
                buffer_index = triple.2;
                struct_arrays.push((struct_field.clone(), triple.0));
            }
            // create struct array from fields, arrays and null data
            let struct_array = crate::array::StructArray::from((
                struct_arrays,
                null_buffer,
                struct_node.null_count() as usize,
            ));
            Arc::new(struct_array)
        }
        _ => {
            let array = create_primitive_array(
                &nodes[node_index],
                data_type,
                buffers[buffer_index..buffer_index + 2]
                    .iter()
                    .map(|buf| read_buffer(buf, data))
                    .collect(),
            );
            node_index = node_index + 1;
            buffer_index = buffer_index + 2;
            array
        }
    };
    (array, node_index, buffer_index)
}

/// Reads the correct number of buffers based on data type and null_count, and creates a
/// primitive array ref
fn create_primitive_array(
    field_node: &ipc::FieldNode,
    data_type: &DataType,
    buffers: Vec<Buffer>,
) -> ArrayRef {
    use DataType::*;
    let length = field_node.length() as usize;
    let null_count = field_node.null_count() as usize;
    let array_data = match data_type {
        Utf8 => {
            // read 3 buffers
            ArrayData::new(
                data_type.clone(),
                length,
                Some(null_count),
                Some(buffers[0].clone()),
                0,
                buffers[1..3].to_vec(),
                vec![],
            )
        }
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32
        | Boolean | Float64 | Time32(_) | Time64(_) | Timestamp(_) | Date32(_)
        | Date64(_) => ArrayData::new(
            data_type.clone(),
            length,
            Some(null_count),
            Some(buffers[0].clone()),
            0,
            buffers[1..].to_vec(),
            vec![],
        ),
        t @ _ => panic!("Data type {:?} either unsupported or not primitive", t),
    };

    crate::array::make_array(Arc::new(array_data))
}

fn create_list_array(
    field_node: &ipc::FieldNode,
    data_type: &DataType,
    buffers: &[Buffer],
    child_array: ArrayRef,
) -> ArrayRef {
    if let &DataType::List(_) = data_type {
        let array_data = ArrayData::new(
            data_type.clone(),
            field_node.length() as usize,
            Some(field_node.null_count() as usize),
            Some(buffers[0].clone()),
            0,
            buffers[1..2].to_vec(),
            vec![child_array.data()],
        );
        crate::array::make_array(Arc::new(array_data))
    } else {
        panic!("Cannot create list array from {:?}", data_type)
    }
}

fn read_record_batch(
    buf: &Vec<u8>,
    batch: ipc::RecordBatch,
    schema: Arc<Schema>,
) -> Result<Option<RecordBatch>> {
    let buffers = batch.buffers().unwrap();
    let field_nodes = batch.nodes().unwrap();
    // keep track of buffer and node index, the functions that create arrays mutate these
    let mut buffer_index = 0;
    let mut node_index = 0;
    let mut arrays = vec![];

    // keep track of index as lists require more than one node
    for field in schema.fields() {
        let triple = create_array(
            field_nodes,
            field.data_type(),
            &buf,
            buffers,
            node_index,
            buffer_index,
        );
        node_index = triple.1;
        buffer_index = triple.2;
        arrays.push(triple.0);
    }

    RecordBatch::try_new(schema.clone(), arrays).map(|batch| Some(batch))
}

/// Arrow File reader
pub struct Reader<R: Read + Seek> {
    /// Buffered reader that supports reading and seeking
    reader: BufReader<R>,
    /// The schema that is read from the file header
    schema: Arc<Schema>,
    /// The blocks in the file
    ///
    /// A block indicates the regions in the file to read to get data
    blocks: Vec<ipc::Block>,
    /// A counter to keep track of the current block that should be read
    current_block: usize,
    /// The total number of blocks, which may contain record batches and other types
    total_blocks: usize,
}

impl<R: Read + Seek> Reader<R> {
    /// Try to create a new reader
    ///
    /// Returns errors if the file does not meet the Arrow Format header and footer
    /// requirements
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
        let meta_len = u32::from_le_bytes(meta_size);

        let mut meta_buffer = vec![0; meta_len as usize];
        reader.seek(SeekFrom::Start(12))?;
        reader.read_exact(&mut meta_buffer)?;

        let vecs = &meta_buffer.to_vec();
        let message = ipc::get_root_as_message(vecs);
        // message header is a Schema, so read it
        let ipc_schema: ipc::Schema = message.header_as_schema().unwrap();
        let schema = ipc::convert::fb_to_schema(ipc_schema);

        // what does the footer contain?
        let mut footer_size: [u8; 4] = [0; 4];
        reader.seek(SeekFrom::End(-10))?;
        reader.read_exact(&mut footer_size)?;
        let footer_len = u32::from_le_bytes(footer_size);

        // read footer
        let mut footer_data = vec![0; footer_len as usize];
        reader.seek(SeekFrom::End(-10 - footer_len as i64))?;
        reader.read_exact(&mut footer_data)?;
        let footer = ipc::get_root_as_footer(&footer_data[..]);

        let blocks = footer.recordBatches().unwrap();

        let total_blocks = blocks.len();

        Ok(Self {
            reader,
            schema: Arc::new(schema),
            blocks: blocks.to_vec(),
            current_block: 0,
            total_blocks,
        })
    }

    /// Return the number of batches in the file
    pub fn num_batches(&self) -> usize {
        self.total_blocks
    }

    /// Return the schema of the file
    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Read the next record batch
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
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

            let message = ipc::get_root_as_message(&block_data[..]);

            match message.header_type() {
                ipc::MessageHeader::Schema => {
                    panic!("Not expecting a schema when messages are read")
                }
                ipc::MessageHeader::DictionaryBatch => {
                    unimplemented!("reading dictionary batches not yet supported")
                }
                ipc::MessageHeader::RecordBatch => {
                    let batch = message.header_as_record_batch().unwrap();
                    // read the block that makes up the record batch into a buffer
                    let mut buf = vec![0; block.bodyLength() as usize];
                    self.reader.seek(SeekFrom::Start(
                        block.offset() as u64 + block.metaDataLength() as u64,
                    ))?;
                    self.reader.read_exact(&mut buf)?;

                    read_record_batch(&buf, batch, self.schema())
                }
                ipc::MessageHeader::SparseTensor => {
                    unimplemented!("reading sparse tensors not yet supported")
                }
                ipc::MessageHeader::Tensor => {
                    unimplemented!("reading tensors not yet supported")
                }
                ipc::MessageHeader::NONE => panic!("unknown message header"),
            }
        } else {
            Ok(None)
        }
    }

    /// Read a specific record batch
    ///
    /// Sets the current block to the batch number, and reads the record batch at that
    /// block
    pub fn read_batch(&mut self, batch_num: usize) -> Result<Option<RecordBatch>> {
        if batch_num >= self.total_blocks {
            Err(ArrowError::IoError(format!(
                "Cannot read batch at index {} from {} total batches",
                batch_num, self.total_blocks
            )))
        } else {
            self.current_block = batch_num;
            self.next()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array::*;
    use crate::builder::{BinaryBuilder, Int32Builder, ListBuilder};
    use crate::datatypes::*;
    use std::fs::File;

    // #[test]
    // fn test_read_primitive_file() {
    //     let file = File::open("./test/data/primitive_types.file.dat").unwrap();

    //     let mut reader = Reader::try_new(file).unwrap();
    //     assert_eq!(3, reader.num_batches());
    //     for _ in 0..reader.num_batches() {
    //         let batch = reader.next().unwrap().unwrap();
    //         validate_batch(batch);
    //     }
    //     // try read a batch after all batches are exhausted
    //     let batch = reader.next().unwrap();
    //     assert!(batch.is_none());

    //     // seek a specific batch
    //     let batch = reader.read_batch(4).unwrap().unwrap();
    //     validate_batch(batch);
    //     // try read a batch after seeking to the last batch
    //     let batch = reader.next().unwrap();
    //     assert!(batch.is_none());
    // }

    #[test]
    fn test_read_struct_file() {
        use DataType::*;
        let file = File::open("./test/data/struct_types.file.dat").unwrap();
        let schema = Schema::new(vec![Field::new(
            "structs",
            Struct(vec![
                Field::new("bools", Boolean, true),
                Field::new("int8s", Int8, true),
                Field::new("varbinary", Utf8, true),
                Field::new("numericlist", List(Box::new(Int32)), true),
            ]),
            false,
        )]);

        // batch contents
        let list_values_builder = Int32Builder::new(10);
        let mut list_builder = ListBuilder::new(list_values_builder);
        // [[1,2,3,4], null, [5,6], [7], [8,9,10]]
        list_builder.values().append_value(1).unwrap();
        list_builder.values().append_value(2).unwrap();
        list_builder.values().append_value(3).unwrap();
        list_builder.values().append_value(4).unwrap();
        list_builder.append(true).unwrap();
        list_builder.append(false).unwrap();
        list_builder.values().append_value(5).unwrap();
        list_builder.values().append_value(6).unwrap();
        list_builder.append(true).unwrap();
        list_builder.values().append_value(7).unwrap();
        list_builder.append(true).unwrap();
        list_builder.values().append_value(8).unwrap();
        list_builder.values().append_value(9).unwrap();
        list_builder.values().append_value(10).unwrap();
        list_builder.append(true).unwrap();
        let _list_array = list_builder.finish();

        let mut binary_builder = BinaryBuilder::new(100);
        binary_builder.append_string("foo").unwrap();
        binary_builder.append_string("bar").unwrap();
        binary_builder.append_string("baz").unwrap();
        binary_builder.append_string("qux").unwrap();
        binary_builder.append_string("quux").unwrap();
        let binary_array = binary_builder.finish();
        // let _struct_array = StructArray::from((
        //     vec![
        //         (
        //             Field::new("bools", Boolean, true),
        //             Arc::new(BooleanArray::from(vec![
        //                 Some(true),
        //                 None,
        //                 None,
        //                 Some(false),
        //                 Some(true),
        //             ])) as Arc<Array>,
        //         ),
        //         (
        //             Field::new("int8s", Int8, true),
        //             Arc::new(Int8Array::from(vec![
        //                 Some(-1),
        //                 None,
        //                 None,
        //                 Some(-4),
        //                 Some(-5),
        //             ])),
        //         ),
        //         (Field::new("varbinary", Utf8, true), Arc::new(binary_array)),
        //         (
        //             Field::new("numericlist", List(Box::new(Int32)), true),
        //             Arc::new(list_array),
        //         ),
        //     ],
        //     Buffer::from([]),
        //     0,
        // ));

        let mut reader = Reader::try_new(file).unwrap();
        assert_eq!(3, reader.num_batches());
        for _ in 0..reader.num_batches() {
            let batch: RecordBatch = reader.next().unwrap().unwrap();
            assert_eq!(&Arc::new(schema.clone()), batch.schema());
            assert_eq!(1, batch.num_columns());
            assert_eq!(5, batch.num_rows());
            let struct_col: &StructArray = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let struct_col_1: &BooleanArray = struct_col
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert_eq!("PrimitiveArray<Boolean>\n[\n  true,\n  null,\n  null,\n  false,\n  true,\n]", format!("{:?}", struct_col_1));
            // TODO failing tests
            // assert_eq!(
            //     struct_col_1.data(),
            //     BooleanArray::from(
            //         vec![Some(true), None, None, Some(false), Some(true),]
            //     )
            //     .data()
            // );
            // assert_eq!(struct_col.data(), struct_array.data());
        }
        // try read a batch after all batches are exhausted
        let batch = reader.next().unwrap();
        assert!(batch.is_none());

        // seek a specific batch
        let batch = reader.read_batch(2).unwrap().unwrap();
        // validate_batch(batch);
        // try read a batch after seeking to the last batch
        let batch = reader.next().unwrap();
        assert!(batch.is_none());
    }

    // fn validate_batch(batch: RecordBatch) {
    //     // primitive batches were created for
    //     assert_eq!(5, batch.num_rows());
    //     assert_eq!(1, batch.num_columns());
    //     let arr_1 = batch.column(0);
    //     let int32_array = arr_1.as_any().downcast_ref::<BooleanArray>().unwrap();
    //     assert_eq!(
    //         "PrimitiveArray<Boolean>\n[\n  true,\n  null,\n  null,\n  false,\n  true,\n]",
    //         format!("{:?}", int32_array)
    //     );
    //     let arr_2 = batch.column(1);
    //     let binary_array = BinaryArray::from(arr_2.data());
    //     assert_eq!("foo", std::str::from_utf8(binary_array.value(0)).unwrap());
    //     assert_eq!("bar", std::str::from_utf8(binary_array.value(1)).unwrap());
    //     assert_eq!("baz", std::str::from_utf8(binary_array.value(2)).unwrap());
    //     assert!(binary_array.is_null(3));
    //     assert_eq!("quux", std::str::from_utf8(binary_array.value(4)).unwrap());
    //     let arr_3 = batch.column(2);
    //     let f32_array = Float32Array::from(arr_3.data());
    //     assert_eq!(
    //         "PrimitiveArray<Float32>\n[\n  1.0,\n  2.0,\n  null,\n  4.0,\n  5.0,\n]",
    //         format!("{:?}", f32_array)
    //     );
    //     let arr_4 = batch.column(3);
    //     let bool_array = BooleanArray::from(arr_4.data());
    //     assert_eq!(
    //         "PrimitiveArray<Boolean>\n[\n  true,\n  null,\n  false,\n  true,\n  false,\n]",
    //         format!("{:?}", bool_array)
    //     );
    // }
}
