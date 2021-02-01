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

//! Contains reader which reads parquet data into arrow array.

use crate::arrow::array_reader::{build_array_reader, ArrayReader, StructArrayReader};
use crate::arrow::schema::parquet_to_arrow_schema;
use crate::arrow::schema::{
    parquet_to_arrow_schema_by_columns, parquet_to_arrow_schema_by_root_columns,
};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ParquetMetaData;
use crate::file::reader::FileReader;
use arrow::datatypes::{DataType as ArrowType, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::{array::StructArray, error::ArrowError};
use std::sync::Arc;

/// Arrow reader api.
/// With this api, user can get arrow schema from parquet file, and read parquet data
/// into arrow arrays.
pub trait ArrowReader {
    type RecordReader: RecordBatchReader;

    /// Read parquet schema and convert it into arrow schema.
    fn get_schema(&mut self) -> Result<Schema>;

    /// Read parquet schema and convert it into arrow schema.
    /// This schema only includes columns identified by `column_indices`.
    /// To select leaf columns (i.e. `a.b.c` instead of `a`), set `leaf_columns = true`
    fn get_schema_by_columns<T>(
        &mut self,
        column_indices: T,
        leaf_columns: bool,
    ) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>;

    /// Returns record batch reader from whole parquet file.
    ///
    /// # Arguments
    ///
    /// `batch_size`: The size of each record batch returned from this reader. Only the
    /// last batch may contain records less than this size, otherwise record batches
    /// returned from this reader should contains exactly `batch_size` elements.
    fn get_record_reader(&mut self, batch_size: usize) -> Result<Self::RecordReader>;

    /// Returns record batch reader whose record batch contains columns identified by
    /// `column_indices`.
    ///
    /// # Arguments
    ///
    /// `column_indices`: The columns that should be included in record batches.
    /// `batch_size`: Please refer to `get_record_reader`.
    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<Self::RecordReader>
    where
        T: IntoIterator<Item = usize>;
}

pub struct ParquetFileArrowReader {
    file_reader: Arc<dyn FileReader>,
}

impl ArrowReader for ParquetFileArrowReader {
    type RecordReader = ParquetRecordBatchReader;

    fn get_schema(&mut self) -> Result<Schema> {
        let file_metadata = self.file_reader.metadata().file_metadata();
        parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
    }

    fn get_schema_by_columns<T>(
        &mut self,
        column_indices: T,
        leaf_columns: bool,
    ) -> Result<Schema>
    where
        T: IntoIterator<Item = usize>,
    {
        let file_metadata = self.file_reader.metadata().file_metadata();
        if leaf_columns {
            parquet_to_arrow_schema_by_columns(
                file_metadata.schema_descr(),
                column_indices,
                file_metadata.key_value_metadata(),
            )
        } else {
            parquet_to_arrow_schema_by_root_columns(
                file_metadata.schema_descr(),
                column_indices,
                file_metadata.key_value_metadata(),
            )
        }
    }

    fn get_record_reader(
        &mut self,
        batch_size: usize,
    ) -> Result<ParquetRecordBatchReader> {
        let column_indices = 0..self
            .file_reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .num_columns();

        self.get_record_reader_by_columns(column_indices, batch_size)
    }

    fn get_record_reader_by_columns<T>(
        &mut self,
        column_indices: T,
        batch_size: usize,
    ) -> Result<ParquetRecordBatchReader>
    where
        T: IntoIterator<Item = usize>,
    {
        let array_reader = build_array_reader(
            self.file_reader
                .metadata()
                .file_metadata()
                .schema_descr_ptr(),
            self.get_schema()?,
            column_indices,
            self.file_reader.clone(),
        )?;

        Ok(ParquetRecordBatchReader::try_new(batch_size, array_reader)?)
    }
}

impl ParquetFileArrowReader {
    pub fn new(file_reader: Arc<dyn FileReader>) -> Self {
        Self { file_reader }
    }

    // Expose the reader metadata
    pub fn get_metadata(&mut self) -> ParquetMetaData {
        self.file_reader.metadata().clone()
    }
}

pub struct ParquetRecordBatchReader {
    batch_size: usize,
    array_reader: Box<dyn ArrayReader>,
    schema: SchemaRef,
}

impl Iterator for ParquetRecordBatchReader {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.array_reader.next_batch(self.batch_size) {
            Err(error) => Some(Err(error.into())),
            Ok(array) => {
                let struct_array =
                    array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                        ArrowError::ParquetError(
                            "Struct array reader should return struct array".to_string(),
                        )
                    });
                match struct_array {
                    Err(err) => Some(Err(err)),
                    Ok(e) => {
                        match RecordBatch::try_new(self.schema.clone(), e.columns_ref()) {
                            Err(err) => Some(Err(err)),
                            Ok(record_batch) => {
                                if record_batch.num_rows() > 0 {
                                    Some(Ok(record_batch))
                                } else {
                                    None
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl RecordBatchReader for ParquetRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ParquetRecordBatchReader {
    pub fn try_new(
        batch_size: usize,
        array_reader: Box<dyn ArrayReader>,
    ) -> Result<Self> {
        // Check that array reader is struct array reader
        array_reader
            .as_any()
            .downcast_ref::<StructArrayReader>()
            .ok_or_else(|| general_err!("The input must be struct array reader!"))?;

        let schema = match array_reader.get_data_type() {
            ArrowType::Struct(ref fields) => Schema::new(fields.clone()),
            _ => unreachable!("Struct array reader's data type is not struct!"),
        };

        Ok(Self {
            batch_size,
            array_reader,
            schema: Arc::new(schema),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::arrow::arrow_reader::{ArrowReader, ParquetFileArrowReader};
    use crate::arrow::converter::{
        Converter, FixedSizeArrayConverter, FromConverter, IntervalDayTimeArrayConverter,
        Utf8ArrayConverter,
    };
    use crate::column::writer::get_typed_column_writer_mut;
    use crate::data_type::{
        BoolType, ByteArray, ByteArrayType, DataType, FixedLenByteArray,
        FixedLenByteArrayType, Int32Type,
    };
    use crate::errors::Result;
    use crate::file::properties::WriterProperties;
    use crate::file::reader::{FileReader, SerializedFileReader};
    use crate::file::writer::{FileWriter, SerializedFileWriter};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::TypePtr;
    use crate::util::test_common::{get_temp_filename, RandGen};
    use arrow::array::*;
    use arrow::record_batch::RecordBatchReader;
    use rand::RngCore;
    use serde_json::json;
    use serde_json::Value::{Array as JArray, Null as JNull, Object as JObject};
    use std::cmp::min;
    use std::convert::TryFrom;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    #[test]
    fn test_arrow_reader_all_columns() {
        let json_values = get_json_array("parquet/generated_simple_numerics/blogs.json");

        let parquet_file_reader =
            get_test_reader("parquet/generated_simple_numerics/blogs.parquet");

        let max_len = parquet_file_reader.metadata().file_metadata().num_rows() as usize;

        let mut arrow_reader = ParquetFileArrowReader::new(parquet_file_reader);

        let mut record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");

        // Verify that the schema was correctly parsed
        let original_schema = arrow_reader.get_schema().unwrap().fields().clone();
        assert_eq!(original_schema, *record_batch_reader.schema().fields());

        compare_batch_json(&mut record_batch_reader, json_values, max_len);
    }

    #[test]
    fn test_arrow_reader_single_column() {
        let json_values = get_json_array("parquet/generated_simple_numerics/blogs.json");

        let projected_json_values = json_values
            .into_iter()
            .map(|value| match value {
                JObject(fields) => {
                    json!({ "blog_id": fields.get("blog_id").unwrap_or(&JNull).clone()})
                }
                _ => panic!("Input should be json object array!"),
            })
            .collect::<Vec<_>>();

        let parquet_file_reader =
            get_test_reader("parquet/generated_simple_numerics/blogs.parquet");

        let max_len = parquet_file_reader.metadata().file_metadata().num_rows() as usize;

        let mut arrow_reader = ParquetFileArrowReader::new(parquet_file_reader);

        let mut record_batch_reader = arrow_reader
            .get_record_reader_by_columns(vec![2], 60)
            .expect("Failed to read into array!");

        // Verify that the schema was correctly parsed
        let original_schema = arrow_reader.get_schema().unwrap().fields().clone();
        assert_eq!(1, record_batch_reader.schema().fields().len());
        assert_eq!(original_schema[1], record_batch_reader.schema().fields()[0]);

        compare_batch_json(&mut record_batch_reader, projected_json_values, max_len);
    }

    #[test]
    fn test_bool_single_column_reader_test() {
        let message_type = "
        message test_schema {
          REQUIRED BOOLEAN leaf;
        }
        ";

        let converter = FromConverter::new();
        run_single_column_reader_tests::<
            BoolType,
            BooleanArray,
            FromConverter<Vec<Option<bool>>, BooleanArray>,
            BoolType,
        >(2, message_type, &converter);
    }

    struct RandFixedLenGen {}

    impl RandGen<FixedLenByteArrayType> for RandFixedLenGen {
        fn gen(len: i32) -> FixedLenByteArray {
            let mut v = vec![0u8; len as usize];
            rand::thread_rng().fill_bytes(&mut v);
            ByteArray::from(v).into()
        }
    }

    #[test]
    fn test_fixed_length_binary_column_reader() {
        let message_type = "
        message test_schema {
          REQUIRED FIXED_LEN_BYTE_ARRAY (20) leaf;
        }
        ";

        let converter = FixedSizeArrayConverter::new(20);
        run_single_column_reader_tests::<
            FixedLenByteArrayType,
            FixedSizeBinaryArray,
            FixedSizeArrayConverter,
            RandFixedLenGen,
        >(20, message_type, &converter);
    }

    #[test]
    fn test_interval_day_time_column_reader() {
        let message_type = "
        message test_schema {
          REQUIRED FIXED_LEN_BYTE_ARRAY (12) leaf (INTERVAL);
        }
        ";

        let converter = IntervalDayTimeArrayConverter {};
        run_single_column_reader_tests::<
            FixedLenByteArrayType,
            IntervalDayTimeArray,
            IntervalDayTimeArrayConverter,
            RandFixedLenGen,
        >(12, message_type, &converter);
    }

    struct RandUtf8Gen {}

    impl RandGen<ByteArrayType> for RandUtf8Gen {
        fn gen(len: i32) -> ByteArray {
            Int32Type::gen(len).to_string().as_str().into()
        }
    }

    #[test]
    fn test_utf8_single_column_reader_test() {
        let message_type = "
        message test_schema {
          REQUIRED BINARY leaf (UTF8);
        }
        ";

        let converter = Utf8ArrayConverter {};
        run_single_column_reader_tests::<
            ByteArrayType,
            StringArray,
            Utf8ArrayConverter,
            RandUtf8Gen,
        >(2, message_type, &converter);
    }

    #[test]
    fn test_read_decimal_file() {
        use arrow::array::DecimalArray;
        let testdata = arrow::util::test_util::parquet_test_data();
        let file_variants = vec![("fixed_length", 25), ("int32", 4), ("int64", 10)];
        for (prefix, target_precision) in file_variants {
            let path = format!("{}/{}_decimal.parquet", testdata, prefix);
            let parquet_reader =
                SerializedFileReader::try_from(File::open(&path).unwrap()).unwrap();
            let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

            let mut record_reader = arrow_reader.get_record_reader(32).unwrap();

            let batch = record_reader.next().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 24);
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<DecimalArray>()
                .unwrap();

            let expected = 1..25;

            assert_eq!(col.precision(), target_precision);
            assert_eq!(col.scale(), 2);

            for (i, v) in expected.enumerate() {
                assert_eq!(col.value(i), v * 100_i128);
            }
        }
    }

    /// Parameters for single_column_reader_test
    #[derive(Debug)]
    struct TestOptions {
        /// Number of row group to write to parquet (row group size =
        /// num_row_groups / num_rows)
        num_row_groups: usize,
        /// Total number of rows
        num_rows: usize,
        /// Size of batches to read back
        record_batch_size: usize,
        /// Total number of batches to attempt to read.
        /// `record_batch_size` * `num_iterations` should be greater
        /// than `num_rows` to ensure the data can be read back completely
        num_iterations: usize,
    }

    /// Create a parquet file and then read it using
    /// `ParquetFileArrowReader` using a standard set of parameters
    /// `opts`.
    ///
    /// `rand_max` represents the maximum size of value to pass to to
    /// value generator
    fn run_single_column_reader_tests<T, A, C, G>(
        rand_max: i32,
        message_type: &str,
        converter: &C,
    ) where
        T: DataType,
        G: RandGen<T>,
        A: PartialEq + Array + 'static,
        C: Converter<Vec<Option<T::T>>, A> + 'static,
    {
        let all_options = vec![
            // choose record_batch_batch (15) so batches cross row
            // group boundaries (50 rows in 2 row groups) cases.
            TestOptions {
                num_row_groups: 2,
                num_rows: 100,
                record_batch_size: 15,
                num_iterations: 50,
            },
            // choose record_batch_batch (5) so batches sometime fall
            // on row group boundaries and (25 rows in 3 row groups
            // --> row groups of 10, 10, and 5). Tests buffer
            // refilling edge cases.
            TestOptions {
                num_row_groups: 3,
                num_rows: 25,
                record_batch_size: 5,
                num_iterations: 50,
            },
            // Choose record_batch_size (25) so all batches fall
            // exactly on row group boundary (25). Tests buffer
            // refilling edge cases.
            TestOptions {
                num_row_groups: 4,
                num_rows: 100,
                record_batch_size: 25,
                num_iterations: 50,
            },
        ];

        all_options.into_iter().for_each(|opts| {
            // Print out options to facilitate debugging failures on CI
            println!("Running with Test Options: {:?}", opts);
            single_column_reader_test::<T, A, C, G>(
                opts,
                rand_max,
                message_type,
                converter,
            )
        });
    }

    /// Create a parquet file and then read it using
    /// `ParquetFileArrowReader` using the parameters described in
    /// `opts`.
    fn single_column_reader_test<T, A, C, G>(
        opts: TestOptions,
        rand_max: i32,
        message_type: &str,
        converter: &C,
    ) where
        T: DataType,
        G: RandGen<T>,
        A: PartialEq + Array + 'static,
        C: Converter<Vec<Option<T::T>>, A> + 'static,
    {
        let values: Vec<Vec<T::T>> = (0..opts.num_row_groups)
            .map(|_| G::gen_vec(rand_max, opts.num_rows))
            .collect();

        let path = get_temp_filename();

        let schema = parse_message_type(message_type).map(Arc::new).unwrap();

        generate_single_column_file_with_data::<T>(&values, path.as_path(), schema)
            .unwrap();

        let parquet_reader =
            SerializedFileReader::try_from(File::open(&path).unwrap()).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

        let mut record_reader = arrow_reader
            .get_record_reader(opts.record_batch_size)
            .unwrap();

        let expected_data: Vec<Option<T::T>> = values
            .iter()
            .flat_map(|v| v.iter())
            .map(|b| Some(b.clone()))
            .collect();

        for i in 0..opts.num_iterations {
            let start = i * opts.record_batch_size;

            let batch = record_reader.next();
            if start < expected_data.len() {
                let end = min(start + opts.record_batch_size, expected_data.len());
                assert!(batch.is_some());

                let mut data = vec![];
                data.extend_from_slice(&expected_data[start..end]);

                assert_eq!(
                    &converter.convert(data).unwrap(),
                    batch
                        .unwrap()
                        .unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<A>()
                        .unwrap()
                );
            } else {
                assert!(batch.is_none());
            }
        }
    }

    fn generate_single_column_file_with_data<T: DataType>(
        values: &[Vec<T::T>],
        path: &Path,
        schema: TypePtr,
    ) -> Result<()> {
        let file = File::create(path)?;
        let writer_props = Arc::new(WriterProperties::builder().build());

        let mut writer = SerializedFileWriter::new(file, schema, writer_props)?;

        for v in values {
            let mut row_group_writer = writer.next_row_group()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .expect("Column writer is none!");

            get_typed_column_writer_mut::<T>(&mut column_writer)
                .write_batch(v, None, None)?;

            row_group_writer.close_column(column_writer)?;
            writer.close_row_group(row_group_writer)?
        }

        writer.close()
    }

    fn get_test_reader(file_name: &str) -> Arc<dyn FileReader> {
        let file = get_test_file(file_name);

        let reader =
            SerializedFileReader::new(file).expect("Failed to create serialized reader");

        Arc::new(reader)
    }

    fn get_test_file(file_name: &str) -> File {
        let mut path = PathBuf::new();
        path.push(arrow::util::test_util::arrow_test_data());
        path.push(file_name);

        File::open(path.as_path()).expect("File not found!")
    }

    fn get_json_array(filename: &str) -> Vec<serde_json::Value> {
        match serde_json::from_reader(get_test_file(filename))
            .expect("Failed to read json value from file!")
        {
            JArray(values) => values,
            _ => panic!("Input should be json array!"),
        }
    }

    fn compare_batch_json(
        record_batch_reader: &mut dyn RecordBatchReader,
        json_values: Vec<serde_json::Value>,
        max_len: usize,
    ) {
        for i in 0..20 {
            let array: Option<StructArray> = record_batch_reader
                .next()
                .map(|r| r.expect("Failed to read record batch!").into());

            let (start, end) = (i * 60_usize, (i + 1) * 60_usize);

            if start < max_len {
                assert!(array.is_some());
                assert_ne!(0, array.as_ref().unwrap().len());
                let end = min(end, max_len);
                let json = JArray(Vec::from(&json_values[start..end]));
                assert_eq!(array.unwrap(), json)
            } else {
                assert!(array.is_none());
            }
        }
    }

    #[test]
    fn test_read_structs() {
        // This particular test file has columns of struct types where there is
        // a column that has the same name as one of the struct fields
        // (see: ARROW-11452)
        let parquet_file_reader = get_test_reader("structs.parquet");
        let mut arrow_reader = ParquetFileArrowReader::new(parquet_file_reader);
        let record_batch_reader = arrow_reader
            .get_record_reader(60)
            .expect("Failed to read into array!");

        for batch in record_batch_reader {
            batch.unwrap();
        }
    }
}
