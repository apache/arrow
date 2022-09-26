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

void print_array(GArrow.Array array) {
    stdout.printf("[");
    var n = array.get_length();

    switch (array.get_value_type()) {
    case GArrow.Type.UINT8:
        var concrete_array = array as GArrow.UInt8Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%hhu", concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.UINT16:
        var concrete_array = array as GArrow.UInt16Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%" + uint16.FORMAT, concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.UINT32:
        var concrete_array = array as GArrow.UInt32Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%" + uint32.FORMAT, concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.UINT64:
        var concrete_array = array as GArrow.UInt64Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%" + uint64.FORMAT, concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.INT8:
        var concrete_array = array as GArrow.Int8Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%hhd", concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.INT16:
        var concrete_array = array as GArrow.Int16Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%" + int16.FORMAT, concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.INT32:
        var concrete_array = array as GArrow.Int32Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%" + int32.FORMAT, concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.INT64:
        var concrete_array = array as GArrow.Int64Array;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%" + int64.FORMAT, concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.FLOAT:
        var concrete_array = array as GArrow.FloatArray;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%g", concrete_array.get_value(i));
        }
        break;
    case GArrow.Type.DOUBLE:
        var concrete_array = array as GArrow.DoubleArray;
        for (var i = 0; i < n; i++) {
            if (i > 0) {
                stdout.printf(", ");
            }
            stdout.printf("%g", concrete_array.get_value(i));
        }
        break;
    default:
        break;
    }

    stdout.printf("]\n");
}

void print_record_batch(GArrow.RecordBatch record_batch) {
    var n_columns = record_batch.get_n_columns();
    for (var nth_column = 0; nth_column < n_columns; nth_column++) {
        stdout.printf("columns[%" + int64.FORMAT + "](%s): ",
                      nth_column,
                      record_batch.get_column_name(nth_column));
        var array = record_batch.get_column_data(nth_column);
        print_array(array);
    }
}

int main (string[] args) {
    var input_path = "/tmp/file.arrow";
    if (args.length > 1) {
        input_path = args[0];
    }

    GArrow.MemoryMappedInputStream input;
    try {
        input = new GArrow.MemoryMappedInputStream(input_path);
    } catch (Error error) {
        stderr.printf("failed to open file: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    {
        GArrow.RecordBatchFileReader reader;
        try {
            var seekable_input = input as GArrow.SeekableInputStream;
            reader = new GArrow.RecordBatchFileReader(seekable_input);
        } catch (Error error) {
            stderr.printf("failed to open file reader: %s\n", error.message);
            return Posix.EXIT_FAILURE;
        }

        var n = reader.get_n_record_batches();
        for (var i = 0; i < n; i++) {
            GArrow.RecordBatch record_batch;
            try {
                record_batch = reader.read_record_batch(i);
            } catch (Error error) {
                stderr.printf("failed to read %u-th record batch: %s\n",
                              i, error.message);
                return Posix.EXIT_FAILURE;
            }
            print_record_batch(record_batch);
        }
    }

    return Posix.EXIT_SUCCESS;
}
