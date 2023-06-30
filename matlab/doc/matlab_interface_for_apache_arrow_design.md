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

# MATLAB Interface for Apache Arrow 

## Overview 
This document outlines a high-level roadmap for development of a [MATLAB] Interface for Apache Arrow, which enables interfacing with [Arrow] memory. 

## Use Cases 
Apache Arrow is designed to enable a variety of high-performance columnar analytics use cases. 

This design document focuses on a subset of use cases that we feel will help to lay the foundation for more advanced [use cases] in the future. 

1. **UC1**: Ability to create, access, and delete Arrow memory using MATLAB code. 
2. **UC2**: Ability to serialize and deserialize Arrow memory using MATLAB code to/from file formats like Parquet, Feather, JSON, and CSV. 
3. **UC3**: Ability to move in-memory tabular data, represented as a MATLAB table, to other languages, like Python, R, and Rust, with minimal overhead (ideally, zero copy). 

## Design 
We envision a set of packaged (`arrow.*`) classes and functions allowing users to interact with key functionality from the Arrow C++ libraries using MATLAB code. 

Included below is a list of example MATLAB and C++ APIs that would be exposed by the MATLAB Interface for Apache Arrow. 

### MATLAB APIs 
- `arrow.Buffer`
- `arrow.Array`
- `arrow.RecordBatch`
- `arrow.Table`
- `arrow.Field`
- `arrow.Schema`
- `arrow.type.DataType`
  - `arrow.type.Float64`
  - `arrow.type.String`
  - `arrow.type.Date`
  - `arrow.type.Time`
  - ... 
- `arrow.memory.getTotalBytesAllocated`
- `arrow.memory.allocateBuffer`
- ... 

### C++ APIs 
In order to enable interaction with the Arrow C++ libraries, the MATLAB Interface for Apache Arrow must expose associated C++ APIs for wrapping/unwrapping MATLAB [`mxArray`] data to/from appropriate Arrow C++ types. 

The list below provides a few brief examples of what these C++ APIs might look like (intended to be consistent with the rest of the Arrow ecosystem). 

- `arrow::matlab::is_array`
- `arrow::matlab::is_record_batch` 
- `arrow::matlab::is_table`
- `arrow::matlab::unwrap_array`
- `arrow::matlab::wrap_array`
- `arrow::matlab::unwrap_record_batch`
- `arrow::matlab::wrap_record_batch`
- `arrow::matlab::unwrap_table`
- `arrow::matlab::wrap_table`
- ... 

## Design Cases 

### Use Case: UC1 
A MATLAB developer could create an `arrow.Array` from an "ordinary" MATLAB array (e.g. a numeric row vector of type `double`). 
They could then operate on this array in a variety of different ways (e.g. indexing/slicing, getting its type/class, clearing it from the workspace, etc.). 
The `arrow.array` “factory function” returns a type-specific, concrete subclass of the abstract `arrow.Array` class based on the MATLAB type of the input array. For example, passing a double array to the `arrow.array` function will return a corresponding `arrow.Float64Array`. 

**Note**: MATLAB [`missing` values] (e.g. `NaN`, `NaT`, `<undefined>`) are automatically converted into Arrow `NULL` values upon construction of an `arrow.Array` subclass instance. 

###### Example Code: 
``` matlab
>> A = randi(100, 1, 5) 
A = 
    82 91 13 92 64 

>> class(A) 
ans = 
    'double' 

>> A(4) = NaN; % Set the fourth element to NaN. 

>> AA = arrow.array(A); % Create an arrow.Array from A. 

>> class(AA) 
ans = 
    'arrow.Float64Array' 

>> AA(3:5) % Extract elements at indices 3 to 5 from AA. 
ans = 
    13 <NULL> 64 

>> clear AA; % Clear AA from workspace and release Arrow C++ memory. 
```

### Use Case: UC2 

#### Developer Workflow for Writing a MATLAB Table to a Feather File 

To serialize MATLAB data to a file on disk (e.g. Feather, Parquet), a MATLAB developer could start by constructing an `arrow.Table` using one of several different approaches. 

They could individually compose the table from a set of `arrow.Array` objects (one for each table variable). 

###### Example Code: 
``` matlab
>> Var1 = arrow.array(["foo"; "bar"; "baz"]); 

>> Var2 = arrow.array([today; today + 1; today + 2]); 

>> Var3 = arrow.array([10; 20; 30]); 

>> AT = arrow.Table(Var1, Var2, Var3); 
```
Alternatively, they could directly convert from an existing MATLAB `table` to an `arrow.Table` using a function like `arrow.matlab2arrow` to convert between an existing MATLAB `table` and an `arrow.Table`. 

###### Example Code: 
``` matlab
>> Weight = [10; 24; 10; 12; 18]; 

>> Radius = [80; 135; 65; 70; 150]; 

>> Density = [10.2; 20.5; 11.2; 13.7; 17.8]; 

>> T = table(Weight, Radius, Density); % Create a MATLAB table 

>> AT = arrow.matlab2arrow(T); % Create an arrow.Table 
```
To serialize the `arrow.Table`, `AT`, to a file (e.g. Feather) on disk, the user could then instantiate an `arrow.FeatherTableWriter`. 

###### Example Code: 
``` matlab
>> featherTableWriter = arrow.FeatherTableWriter(); 

>> featherTableWriter.write(AT, "data.feather"); 
```
The Feather file could then be read and operated on by an external process like Rust or Go. To read it back into MATLAB after modification by another process, the user could instantiate an `arrow.FeatherTableReader`. 

###### Example Code: 
``` matlab
>> featherTableReader = arrow.FeatherTableReader("data.feather"); 

>> AT = featherTableReader.read(); 
```
#### Advanced MATLAB User Workflow for Implementing Support for Writing to Feather Files 

To add support for writing to Feather files, an advanced MATLAB user could use the MATLAB and C++ APIs offered by the MATLAB Interface for Apache Arrow to create `arrow.FeatherTableWriter`. 

They would need to author a [MEX function] (e.g. `featherwriteMEX`), which can be called directly by MATLAB code. Within their MEX function, they could use `arrow::matlab::unwrap_table` to convert between the MATLAB representation of the Arrow memory (`arrow.Table`) and the equivalent C++ representation (`arrow::Table`). Once the `arrow.Table` has been "unwrapped" into a C++ `arrow::Table`, it can be passed to the appropriate Arrow C++ library API for writing to a Feather file (`arrow::ipc::feather::WriteTable`). 

An analogous workflow could be followed to create `arrow.FeatherTableReader` to enable reading from Feather files. 

#### Enabling High-Level Workflows 

Ultimately, many of the APIs exposed by the MATLAB Interface for Apache Arrow are targeted at advanced MATLAB users. By leveraging these building blocks, advanced MATLAB users can create high-level interfaces, which are useful to everyday MATLAB users. An example of such a high-level interface would be `featherwrite`, intended to make it easy to write Feather files. A diagram summarizing the overall workflow and specific pieces an advanced user would need to author to create such a high-level interface is included below.

![Code flow diagram](https://github.com/mathworks/matlab-arrow-support-files/raw/main/images/design_doc_code_flow_diagram.svg)

### Use Case: UC3 

Arrow supports several approaches to sharing memory locally. 

Roughly speaking, local memory sharing workflows can be divided into two categories: 
1. In-Process Memory Sharing 
2. Out-of-Process Memory Sharing 

#### In-Process Memory Sharing 

[MATLAB supports running Python code within the MATLAB process]. In theory, because MATLAB and Python can share the same virtual address space, users should be able to share Arrow memory efficiently between MATLAB and PyArrow code. The [Apache Arrow C Data Interface] defines a lightweight C API for sharing Arrow data and metadata between multiple languages running within the same virtual address space. 

To share a MATLAB `arrow.Array` with PyArrow efficiently, a user could use the `exportToCDataInterface` method to export the Arrow memory wrapped by an `arrow.Array` to the C Data Interface format, consisting of two C-style structs, [`ArrowArray`] and [`ArrowSchema`], which represent the Arrow data and associated metadata. 

Memory addresses to the `ArrowArray` and `ArrowSchema` structs are returned by the call to `exportToCDataInterface`. These addresses can be passed to Python directly, without having to make any copies of the underlying Arrow data structures that they refer to. A user can then wrap the underlying data pointed to by the `ArrowArray` struct (which is already in the [Arrow Columnar Format]), as well as extract the necessary metadata from the `ArrowSchema` struct, to create a `pyarrow.Array` by using the static method `py.pyarrow.Array._import_from_c`. 

###### Example Code: 
``` matlab
% Create a MATLAB arrow.Array. 
>> AA = arrow.array([1, 2, 3, 4, 5]); 

% Export the MATLAB arrow.Array to the C Data Interface format, returning the 
% memory addresses of the required ArrowArray and ArrowSchema C-style structs. 
>> [arrayMemoryAddress, schemaMemoryAddress] = AA.exportToCDataInterface(); 

% Import the memory addresses of the C Data Interface format structs to create a pyarrow.Array. 
>> PA = py.pyarrow.Array._import_from_c(arrayMemoryAddress, schemaMemoryAddress); 
```
Conversely, a user can create an Arrow array using PyArrow and share it with MATLAB. To do this, they can call the method `_export_to_c` to export a `pyarrow.Array` to the C Data Interface format. 

The memory addresses to the `ArrowArray` and `ArrowSchema` structs populated by the call to `_export_to_c` can be passed to the static method `arrow.Array.importFromCDataInterface` to construct a MATLAB `arrow.Array` with zero copies. 

The example code below is adapted from the [`test_cffi.py` test cases for PyArrow]. 

###### Example Code: 
``` matlab
% Make a pyarrow.Array. 
>> PA = py.pyarrow.array([1, 2, 3, 4, 5]); 

% Create ArrowArray and ArrowSchema C-style structs adhering to the Arrow C Data Interface format. 
>> array = py.pyarrow.cffi.ffi.new("struct ArrowArray*") 

>> arrayMemoryAddress = py.int(py.pyarrow.cffi.ffi.cast("uintptr_t", array)); 

>> schema = py.pyarrow.cffi.ffi.new("struct ArrowSchema*") 

>> schemaMemoryAddress = py.int(py.pyarrow.cffi.ffi.cast("uintptr_t", schema)); 

% Export the pyarrow.Array to the C Data Interface format, populating the required ArrowArray and ArrowShema structs. 
>> PA.export_to_c(arrayMemoryAddress, schemaMemoryAddress) 

% Import the C Data Interface structs to create a MATLAB arrow.Array. 
>> AA = arrow.Array.importFromCDataInterface(arrayMemoryAddress, schemaMemoryAddress); 
```

#### Out-of-Process Memory Sharing 

[MATLAB supports running Python code in a separate process]. A user could leverage the MATLAB Interface for Apache Arrow to share Arrow memory between MATLAB and PyArrow running within a separate Python process using one of the following approaches described below. 

##### Memory-Mapped IPC File 

For large tables used in a multi-process "data processing pipeline", a user could serialize their `arrow.Table` to the Arrow IPC File Format. Then, this file could be memory-mapped (zero-copy) by PyArrow running in a separate process to read the data in with minimal overhead. The fact that the Arrow IPC File Format is a 1:1 mapping of the in-memory Arrow format on disk, makes the memory-mapping highly performant as no custom deserialization/conversion is required to construct a `pyarrow.Table`. 

###### Example Code: 
``` matlab
% Create a MATLAB arrow.Table. 
>> Var1 = arrow.array(["foo", "bar", "baz"]); 

>> Var2 = arrow.array([today, today + 1, today + 2]); 

>> Var3 = arrow.array([10, 20, 30]); 

>> AT = arrow.Table(Var1, Var2, Var3); 

% Write the MATLAB arrow.Table to the Arrow IPC File Format on disk. 
>> arrow.ipcwrite(AT, "data.arrow"); 

% Run Python in a separate process. 
>> pyenv("ExecutionMode", "OutOfProcess");  

% Memory map the Arrow IPC File. 
>> memoryMappedFile = py.pyarrow.memory_map("data.arrow"); 

% Construct pyarrow.ipc.RecordBatchFileReader to read the Arrow IPC File. 
>> recordBatchFileReader = py.pyarrow.ipc.open_file(memoryMappedFile); 

% Read all record batches from the Arrow IPC File in one-shot and return a pyarrow.Table. 
>> PAT = recordBatchFileReader.read_all() 
```

## Testing 
To ensure code quality, we would like to include the following testing infrastructure, at a minimum: 
1. C++ APIs 
   - GoogleTest C++ Unit Tests 
   - Integration with CI workflows 
2. MATLAB APIs  
   - [MATLAB Class-Based Unit Tests] 
   - Integration with CI workflows 
3. [Integration Testing]

## Documentation 
To ensure usability, discoverability, and accessibility, we would like to include high quality documentation for the MATLAB Interface for Apache Arrow. 

Specific areas of documentation would include: 
1. [MATLAB Help Text] for MATLAB APIs
2. MATLAB API reference
3. Usage examples of MATLAB and C++ APIs
4. README for building and installation 
5. Build system documentation 
6. CI integration documentation 

## Installation 
We would ideally like to make it as easy as possible for MATLAB users to install the MATLAB Interface for Apache Arrow without the need to compile [MEX] functions or perform any other manual configuration steps. 

In MATLAB, users normally install optional software packages via the [Add-On Explorer]. This workflow is analogous to the way a [JavaScript user] would install the [`apache-arrow` package via the `npm` package manager] or the way a [Rust user] would install the [`arrow` crate via the `cargo` package manager]. 

In the short term, in the absence of an easily installable MATLAB Add-On, we plan to maintain up-to-date, clearly explained, build and installation instructions for recent versions of MATLAB on GitHub. 

In addition, we'd like to include pre-built MEX functions for Windows, Mac, and Linux that get built regularly via CI workflows. This would allow users to try out the latest functionality without having to manually build the MEX interfaces from scratch. 

## Roadmap 
The table below provides a high-level roadmap for the development of specific capabilities in the MATLAB Interface for Apache Arrow. 

| Capability                       | Use Case | Timeframe |
|----------------------------------|----------|-----------|
| Arrow Memory Interaction         | UC1      | Near Term |
| File Reading/Writing             | UC2      | Near Term |
| In/Out-of-Process Memory Sharing | UC3      | Mid Term  |

<!-- Links -->
[MATLAB]: https://www.mathworks.com/products/matlab.html
[Arrow]: https://arrow.apache.org/
[use cases]: https://arrow.apache.org/use_cases/
[`mxArray`]: https://www.mathworks.com/help/matlab/matlab_external/matlab-data.html
['missing' values]: https://www.mathworks.com/help/matlab/data_analysis/missing-data-in-matlab.html
[MEX function]: https://www.mathworks.com/help/matlab/call-mex-file-functions.html
[several approaches to sharing memory locally]: https://arrow.apache.org/use_cases/#sharing-memory-locally
[MATLAB supports running Python code within the MATLAB process]: https://www.mathworks.com/help/matlab/matlab_external/create-object-from-python-class.html
[Apache Arrow C Data Interface]: https://arrow.apache.org/docs/format/CDataInterface.html
[`ArrowArray`]: https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure
[`ArrowSchema`]: https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure
[Arrow Columnar Format]: https://arrow.apache.org/docs/format/Columnar.html
[`test_cffi.py` test cases for PyArrow]: https://github.com/apache/arrow/blob/97879eb970bac52d93d2247200b9ca7acf6f3f93/python/pyarrow/tests/test_cffi.py#L109
[MATLAB supports running Python code in a separate process]: https://www.mathworks.com/help/matlab/matlab_external/out-of-process-execution-of-python-functionality.html
[MATLAB Class-Based Unit Tests]: https://www.mathworks.com/help/matlab/class-based-unit-tests.html
[Integration Testing]: https://arrow.apache.org/docs/format/Integration.html
[MATLAB Help Text]: https://www.mathworks.com/help/matlab/matlab_prog/add-help-for-your-program.html
[MEX]: https://www.mathworks.com/help/matlab/call-mex-files-1.html
[Add-On Explorer]: https://www.mathworks.com/help/matlab/matlab_env/get-add-ons.html
[JavaScript user]: https://github.com/apache/arrow/tree/main/js
[`apache-arrow` package via the `npm` package manager]: https://www.npmjs.com/package/apache-arrow
[Rust user]: https://github.com/apache/arrow-rs
[`arrow` crate via the `cargo` package manager]: https://crates.io/crates/arrow
