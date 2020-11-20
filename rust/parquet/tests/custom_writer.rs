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

use std::fs::File;
use std::{
    fs,
    io::{prelude::*, SeekFrom},
    sync::Arc,
};

use parquet::file::writer::TryClone;
use parquet::{
    basic::Repetition, basic::Type, file::properties::WriterProperties,
    file::writer::SerializedFileWriter, schema::types,
};
use std::env;

// Test creating some sort of custom writer to ensure the
// appropriate traits are exposed
struct CustomWriter {
    file: File,
}

impl Write for CustomWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Seek for CustomWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

impl TryClone for CustomWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        use std::io::{Error, ErrorKind};
        Err(Error::new(ErrorKind::Other, "Clone not supported"))
    }
}

#[test]
fn test_custom_writer() {
    let schema = Arc::new(
        types::Type::group_type_builder("schema")
            .with_fields(&mut vec![Arc::new(
                types::Type::primitive_type_builder("col1", Type::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )])
            .build()
            .unwrap(),
    );
    let props = Arc::new(WriterProperties::builder().build());

    let file = get_temp_file("test_custom_file_writer");
    let test_file = file.try_clone().unwrap();

    let writer = CustomWriter { file };

    // test is that this file can be created
    let file_writer = SerializedFileWriter::new(writer, schema, props).unwrap();
    std::mem::drop(file_writer);

    // ensure the file now exists and has non zero size
    let metadata = test_file.metadata().unwrap();
    assert!(metadata.len() > 0);
}

/// Returns file handle for a temp file in 'target' directory with a provided content
fn get_temp_file(file_name: &str) -> fs::File {
    // build tmp path to a file in "target/debug/testdata"
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("target");
    path_buf.push("debug");
    path_buf.push("testdata");
    fs::create_dir_all(&path_buf).unwrap();
    path_buf.push(file_name);

    File::create(path_buf).unwrap()
}
