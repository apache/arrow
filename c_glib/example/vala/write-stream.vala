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

int main (string[] args) {
    var output_path = "/tmp/stream.arrow";
    if (args.length > 1) {
        output_path = args[0];
    }

    var fields = new GLib.List<GArrow.Field>();
    fields.append(new GArrow.Field("uint8", new GArrow.UInt8DataType()));
    fields.append(new GArrow.Field("uint16", new GArrow.UInt16DataType()));
    fields.append(new GArrow.Field("uint32", new GArrow.UInt32DataType()));
    fields.append(new GArrow.Field("uint64", new GArrow.UInt64DataType()));
    fields.append(new GArrow.Field("int8", new GArrow.Int8DataType()));
    fields.append(new GArrow.Field("int16", new GArrow.Int16DataType()));
    fields.append(new GArrow.Field("int32", new GArrow.Int32DataType()));
    fields.append(new GArrow.Field("int64", new GArrow.Int64DataType()));
    fields.append(new GArrow.Field("float", new GArrow.FloatDataType()));
    fields.append(new GArrow.Field("double", new GArrow.DoubleDataType()));
    var schema = new GArrow.Schema(fields);

    GArrow.FileOutputStream output;
    try {
        output = new GArrow.FileOutputStream(output_path, false);
    } catch (Error error) {
        stderr.printf("failed to open output path: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    GArrow.RecordBatchStreamWriter writer;
    try {
        writer = new GArrow.RecordBatchStreamWriter(output, schema);
    } catch (Error error) {
        stderr.printf("failed to create writer: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    var n_rows = 4;
    var columns = new GLib.List<GArrow.Array>();
    try {
        var builder = new GArrow.UInt8ArrayBuilder();
        builder.append_values({1, 2, 4, 8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.UInt16ArrayBuilder();
        builder.append_values({1, 2, 4, 8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.UInt32ArrayBuilder();
        builder.append_values({1, 2, 4, 8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.UInt64ArrayBuilder();
        builder.append_values({1, 2, 4, 8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.Int8ArrayBuilder();
        builder.append_values({1, -2, 4, -8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.Int16ArrayBuilder();
        builder.append_values({1, -2, 4, -8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.Int32ArrayBuilder();
        builder.append_values({1, -2, 4, -8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.Int64ArrayBuilder();
        builder.append_values({1, -2, 4, -8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.FloatArrayBuilder();
        builder.append_values({1.1f, -2.2f, 4.4f, -8.8f}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }
    try {
        var builder = new GArrow.DoubleArrayBuilder();
        builder.append_values({1.1, -2.2, 4.4, -8.8}, null);
        columns.append(builder.finish());
    } catch (Error error) {
        stderr.printf("failed to build an array: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    try {
        var record_batch = new GArrow.RecordBatch(schema, n_rows, columns);
        writer.write_record_batch(record_batch);
    } catch (Error error) {
        stderr.printf("failed to build a record batch: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    try {
        writer.close();
        output.close();
    } catch (Error error) {
        stderr.printf("failed to close: %s\n", error.message);
        return Posix.EXIT_FAILURE;
    }

    return Posix.EXIT_SUCCESS;
}
