<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Arrow

An implementation of Arrow targeting .NET Standard.

This implementation is under development and may not be suitable for use in production environments.

# Implementation

- Arrow 0.11 (specification)
- C# 7.2
- .NET Standard 1.3
- Asynchronous I/O
- Uses modern .NET runtime features such as **Span&lt;T&gt;**, **Memory&lt;T&gt;**, **MemoryManager&lt;T&gt;**, and **System.Buffers** primitives for memory allocation, memory storage, and fast serialization.
- Uses **Acyclic Visitor Pattern** for array types and arrays to facilitate serialization, record batch traversal, and format growth.

# Known Issues

- Can not read Arrow files containing dictionary batches, tensors, or tables.
- Can not easily modify allocation strategy without implementing a custom memory pool. All allocations are currently 64-byte aligned and padded to 8-bytes.
- Default memory allocation strategy uses an over-allocation strategy with pointer fixing, which results in significant memory overhead for small buffers. A buffer that requires a single byte for storage may be backed by an allocation of up to 64-bytes to satisfy alignment requirements.
- There are currently few builder APIs available for specific array types. Arrays must be built manually with an arrow buffer builder abstraction.
- FlatBuffer code generation is not included in the build process.
- Serialization implementation does not perform exhaustive validation checks during deserialization in every scenario.
- Throws exceptions with vague, inconsistent, or non-localized messages in many situations
- Throws exceptions that are non-specific to the Arrow implementation in some circumstances where it probably should (eg. does not throw ArrowException exceptions)
- Lack of code documentation
- Lack of usage examples
- Lack of comprehensive unit tests
- Lack of comprehensive benchmarks

# Usage

	using System.Diagnostics;
	using System.IO;
	using System.Threading.Tasks;
	using Apache.Arrow;
	using Apache.Arrow.Ipc;

    public static async Task<RecordBatch> ReadArrowAsync(string filename)
    {
        using (var stream = File.OpenRead("test.arrow"))
        using (var reader = new ArrowFileReader(stream))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Debug.WriteLine("Read record batch with {0} column(s)", recordBatch.ColumnCount);
            return recordBatch;
        }
    }


# Status

## Memory Management

- Allocations are 64-byte aligned and padded to 8-bytes.
- Allocations are automatically garbage collected

## Arrays

### Primitive Types

- Int8, Int16, Int32, Int64
- UInt8, UInt16, UInt32, UInt64
- Float, Double
- Binary (variable-length)
- String (utf-8)
- Null

### Parametric Types

- Timestamp
- Date32
- Date64
- Time32
- Time64
- Binary (fixed-length)
- List

### Type Metadata

- Data Types
- Fields
- Schema

### Serialization

- File
- Stream

## Not Implemented

- Serialization
    - Exhaustive validation
    - Dictionary Batch
        - Can not serialize or deserialize files or streams containing dictionary batches
    - Dictionary Encoding
	- Schema Metadata
	- Schema Field Metadata
- Types
    - Tensor
    - Table
- Arrays
    - Struct
    - Union
        - Dense
        - Sparse
    - Half-Float
    - Decimal
    - Dictionary
- Array Operations
	- Equality / Comparison
	- Casting
	- Builders
- Compute
    - There is currently no API available for a compute / kernel abstraction.

# Build

Install the latest `.NET Core SDK` from https://dotnet.microsoft.com/download.

    dotnet build

## NuGet Build

To build the NuGet package run the following command to build a debug flavor, preview package into the **artifacts** folder.

    dotnet pack

When building the officially released version run: (see Note below about current `git` repository)

    dotnet pack -c Release

Which will build the final/stable package.

NOTE: When building the officially released version, ensure that your `git` repository has the `origin` remote set to `https://github.com/apache/arrow.git`, which will ensure Source Link is set correctly. See https://github.com/dotnet/sourcelink/blob/master/docs/README.md for more information.

There are two output artifacts:
1. `Apache.Arrow.<version>.nupkg` - this contains the exectuable assemblies
2. `Apache.Arrow.<version>.snupkg` - this contains the debug symbols files

Both of these artifacts can then be uploaded to https://www.nuget.org/packages/manage/upload.

## Docker Build

Build from the Apache Arrow project root.

    docker build -f csharp/build/docker/Dockerfile .

## Testing

	dotnet test

All build artifacts are placed in the **artifacts** folder in the project root.

# Updating FlatBuffers code

See https://google.github.io/flatbuffers/flatbuffers_guide_use_java_c-sharp.html for how to get the `flatc` executable.

Run `flatc --csharp` on each `.fbs` file in the [format](../format) folder. And replace the checked in `.cs` files under [FlatBuf](src/Apache.Arrow/Flatbuf) with the generated files.

Update the non-generated [FlatBuffers](src/Apache.Arrow/Flatbuf/FlatBuffers) `.cs` files with the files from the [google/flatbuffers repo](https://github.com/google/flatbuffers/tree/master/net/FlatBuffers).
