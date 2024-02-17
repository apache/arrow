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

# MATLAB Interface to Apache Arrow

## Status

> **Warning** The MATLAB interface is under active development and should be considered experimental.

This is a very early stage MATLAB interface to the Apache Arrow C++ libraries.

Currently, the MATLAB interface supports:

1. Converting between a subset of Arrow `Array` types and MATLAB array types (see table below)
2. Converting between MATLAB `table`s and `arrow.tabular.RecordBatch`s
3. Creating Arrow `Field`s, `Schema`s, and `Type`s
4. Reading and writing Feather V1 files

Supported `arrow.array.Array` types are included in the table below.

**NOTE**: All Arrow `Array` classes listed below are part of the `arrow.array` package (e.g. `arrow.array.Float64Array`).

| MATLAB Array Type | Arrow Array Type |
| ----------------- | ---------------- |
| `uint8`           | `UInt8Array`     |
| `uint16`          | `UInt16Array`    |
| `uint32`          | `UInt32Array`    |
| `uint64`          | `UInt64Array`    |
| `int8`            | `Int8Array`      |
| `int16`           | `Int16Array`     |
| `int32`           | `Int32Array`     |
| `int64`           | `Int64Array`     |
| `single`          | `Float32Array`   |
| `double`          | `Float64Array`   |
| `logical`         | `BooleanArray`   |
| `string`          | `StringArray`    |
| `datetime`        | `TimestampArray` |
| `duration`        | `Time32Array`    |
| `duration`        | `Time64Array`    |

## Prerequisites

To build the MATLAB Interface to Apache Arrow from source, the following software must be installed on the target machine:

1. [MATLAB](https://www.mathworks.com/products/get-matlab.html)
2. [CMake](https://cmake.org/cmake/help/latest/)
3. C++ compiler which supports C++17 (e.g. [`gcc`](https://gcc.gnu.org/) on Linux, [`Xcode`](https://developer.apple.com/xcode/) on macOS, or [`Visual Studio`](https://visualstudio.microsoft.com/) on Windows)
4. [Git](https://git-scm.com/)

## Setup

To set up a local working copy of the source code, start by cloning the [`apache/arrow`](https://github.com/apache/arrow) GitHub repository using [Git](https://git-scm.com/):

```console
$ git clone https://github.com/apache/arrow.git
```

After cloning, change the working directory to the `matlab` subdirectory:

```console
$ cd arrow/matlab
```

## Build

To build the MATLAB interface, use [CMake](https://cmake.org/cmake/help/latest/):

```console
$ cmake -S . -B build
$ cmake --build build --config Release
```

## Install

To install the MATLAB interface to the default software installation location for the target machine (e.g. `/usr/local` on Linux or `C:\Program Files` on Windows), pass the `--target install` flag to CMake.

```console
$ cmake --build build --config Release --target install
```

As part of the install step, the installation directory is added to the [MATLAB Search Path](https://mathworks.com/help/matlab/matlab_env/what-is-the-matlab-search-path.html).

**Note**: This step may fail if the current user is lacking necessary filesystem permissions. If the install step fails, the installation directory can be manually added to the MATLAB Search Path using the [`addpath`](https://www.mathworks.com/help/matlab/ref/addpath.html) command. 

## Test

To run the MATLAB tests, start MATLAB in the `arrow/matlab` directory and call the [`runtests`](https://mathworks.com/help/matlab/ref/runtests.html) command on the `test` directory with `IncludeSubFolders=true`:

``` matlab
>> runtests("test", IncludeSubFolders=true);
```

## Usage

Included below are some example code snippets that illustrate how to use the MATLAB interface.

### Arrow `Array` classes (i.e. `arrow.array.<Array>`)

#### Create an Arrow `Float64Array` from a MATLAB `double` array

```matlab
>> matlabArray = double([1, 2, 3])

matlabArray =

     1     2     3

>> arrowArray = arrow.array(matlabArray)

arrowArray = 

[
  1,
  2,
  3
]
```

#### Create a MATLAB `logical` array from an Arrow `BooleanArray`

```matlab
>> arrowArray = arrow.array([true, false, true])

arrowArray = 

[
  true,
  false,
  true
]

>> matlabArray = toMATLAB(arrowArray)

matlabArray =

  3×1 logical array

   1
   0
   1
```

#### Specify `Null` Values when constructing an `arrow.array.Int8Array`

```matlab
>> matlabArray = int8([122, -1, 456, -10, 789])

matlabArray =

  1×5 int8 row vector

    122     -1    127    -10    127

% Treat all negative array elements as Null
>> validElements = matlabArray > 0

validElements =

  1×5 logical array

   1   0   1   0   1

% Specify which values are Null/Valid by supplying a logical validity "mask"
>> arrowArray = arrow.array(matlabArray, Valid=validElements)

arrowArray = 

[
  122,
  null,
  127,
  null,
  127
]
```

### Arrow `RecordBatch` class

#### Create an Arrow `RecordBatch` from a MATLAB `table`

```matlab
>> matlabTable = table(["A"; "B"; "C"], [1; 2; 3], [true; false; true])

matlabTable =

  3x3 table

    Var1    Var2    Var3
    ____    ____    _____

    "A"      1      true
    "B"      2      false
    "C"      3      true

>> arrowRecordBatch = arrow.recordBatch(matlabTable)

arrowRecordBatch =

Var1:   [
    "A",
    "B",
    "C"
  ]
Var2:   [
    1,
    2,
    3
  ]
Var3:   [
    true,
    false,
    true
  ]
```

#### Create a MATLAB `table` from an Arrow `RecordBatch`

```matlab
>> arrowRecordBatch

arrowRecordBatch =

Var1:   [
    "A",
    "B",
    "C"
  ]
Var2:   [
    1,
    2,
    3
  ]
Var3:   [
    true,
    false,
    true
  ]

>> matlabTable = table(arrowRecordBatch)

matlabTable =

  3x3 table

    Var1    Var2    Var3
    ____    ____    _____

    "A"      1      true
    "B"      2      false
    "C"      3      true
```

#### Create an Arrow `RecordBatch` from multiple Arrow `Array`s


```matlab
>> stringArray = arrow.array(["A", "B", "C"])

stringArray =

[
  "A",
  "B",
  "C"
]

>> timestampArray = arrow.array([datetime(1997, 01, 01), datetime(1998, 01, 01), datetime(1999, 01, 01)])

timestampArray =

[
  1997-01-01 00:00:00.000000,
  1998-01-01 00:00:00.000000,
  1999-01-01 00:00:00.000000
]

>> booleanArray = arrow.array([true, false, true])

booleanArray =

[
  true,
  false,
  true
]

>> arrowRecordBatch = arrow.tabular.RecordBatch.fromArrays(stringArray, timestampArray, booleanArray)

arrowRecordBatch =

Column1:   [
    "A",
    "B",
    "C"
  ]
Column2:   [
    1997-01-01 00:00:00.000000,
    1998-01-01 00:00:00.000000,
    1999-01-01 00:00:00.000000
  ]
Column3:   [
    true,
    false,
    true
  ]
```

#### Extract a column from a `RecordBatch` by index

```matlab
>> arrowRecordBatch = arrow.tabular.RecordBatch.fromArrays(stringArray, timestampArray, booleanArray)

arrowRecordBatch =

Column1:   [
    "A",
    "B",
    "C"
  ]
Column2:   [
    1997-01-01 00:00:00.000000,
    1998-01-01 00:00:00.000000,
    1999-01-01 00:00:00.000000
  ]
Column3:   [
    true,
    false,
    true
  ]

>> timestampArray = arrowRecordBatch.column(2)

timestampArray =

[
  1997-01-01 00:00:00.000000,
  1998-01-01 00:00:00.000000,
  1999-01-01 00:00:00.000000
]
```

### Arrow `Type` classes (i.e. `arrow.type.<Type>`)

#### Create an Arrow `Int8Type` object

```matlab
>> type = arrow.int8()

type =

  Int8Type with properties:

    ID: Int8
```

#### Create an Arrow `TimestampType` object with a specific `TimeUnit` and `TimeZone`

```matlab
>> type = arrow.timestamp(TimeUnit="Second", TimeZone="Asia/Kolkata")

type =

  TimestampType with properties:

          ID: Timestamp
    TimeUnit: Second
    TimeZone: "Asia/Kolkata"
```


#### Get the type enumeration `ID` for an Arrow `Type` object

```matlab
>> type.ID

ans =

  ID enumeration

    Timestamp

>> type = arrow.string()

type =

  StringType with properties:

    ID: String

>> type.ID

ans =

  ID enumeration

    String
```

### Arrow `Field` class

#### Create an Arrow `Field` with type `Int8Type`

```matlab
>> field = arrow.field("Number", arrow.int8())

field =

Number: int8

>> field.Name

ans =

    "Number"

>> field.Type

ans =

  Int8Type with properties:

    ID: Int8

```

#### Create an Arrow `Field` with type `StringType`

```matlab
>> field = arrow.field("Letter", arrow.string())

field =

Letter: string

>> field.Name

ans =

    "Letter"

>> field.Type

ans =

  StringType with properties:

    ID: String
```

#### Extract an Arrow `Field` from an Arrow `Schema` by index

```matlab
>> arrowSchema

arrowSchema =

Letter: string
Number: double

% Specify the field to extract by its index (i.e. 2)
>> field = arrowSchema.field(2)

field =

Number: double
```

#### Extract an Arrow `Field` from an Arrow `Schema` by name

```matlab
>> arrowSchema

arrowSchema =

Letter: string
Number: double

% Specify the field to extract by its name (i.e. "Letter")
>> field = arrowSchema.field("Letter")

field =

Letter: string
```

### Arrow `Schema` class

#### Create an Arrow `Schema` from multiple Arrow `Field`s

```matlab
>> letter = arrow.field("Letter", arrow.string())

letter =

Letter: string

>> number = arrow.field("Number", arrow.int8())

number =

Number: int8

>> schema = arrow.schema([letter, number])

schema =

Letter: string
Number: int8
```

#### Get the `Schema` of an Arrow `RecordBatch`

```matlab
>> matlabTable = table(["A"; "B"; "C"], [1; 2; 3], VariableNames=["Letter", "Number"])

matlabTable =

  3x2 table

    Letter    Number
    ______    ______

     "A"        1
     "B"        2
     "C"        3

>> arrowRecordBatch = arrow.recordBatch(matlabTable)

arrowRecordBatch =

Letter:   [
    "A",
    "B",
    "C"
  ]
Number:   [
    1,
    2,
    3
  ]

>> arrowSchema = arrowRecordBatch.Schema

arrowSchema =

Letter: string
Number: double
```

### Feather V1

#### Write a MATLAB table to a Feather V1 file

``` matlab
>> t = table(["A"; "B"; "C"], [1; 2; 3], [true; false; true])

t =

  3×3 table

    Var1    Var2    Var3
    ____    ____    _____

    "A"      1      true
    "B"      2      false
    "C"      3      true

>> filename = "table.feather";

>> featherwrite(filename, t)
```

#### Read a Feather V1 file into a MATLAB table

``` matlab
>> filename = "table.feather";

>> t = featherread(filename)

t =

  3×3 table

    Var1    Var2    Var3
    ____    ____    _____

    "A"      1      true
    "B"      2      false
    "C"      3      true
```

