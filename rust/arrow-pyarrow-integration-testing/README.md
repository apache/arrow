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

# Arrow c integration

This is a Rust crate that tests compatibility between Rust's Arrow implementation and PyArrow.

Note that this crate uses two languages and an external ABI:
* `Rust`
* `Python`
* C ABI privately exposed by `Pyarrow`.

## Basic idea

Pyarrow exposes a C ABI to convert arrow arrays from and to its C implementation, see [here](https://arrow.apache.org/docs/format/CDataInterface.html).

This package uses the equivalent struct in Rust (`arrow::array::ArrowArray`), and verifies that
we can use pyarrow's interface to move pointers from and to Rust.

## Relevant literature

* [Arrow's CDataInterface](https://arrow.apache.org/docs/format/CDataInterface.html)
* [Rust's FFI](https://doc.rust-lang.org/nomicon/ffi.html)
* [Pyarrow private binds](https://github.com/apache/arrow/blob/ae1d24efcc3f1ac2a876d8d9f544a34eb04ae874/python/pyarrow/array.pxi#L1226)
* [PyO3](https://docs.rs/pyo3/0.12.1/pyo3/index.html)

## How to develop

```bash
# prepare development environment (used to build wheel / install in development)
python -m venv venv
venv/bin/pip install maturin==0.8.2 toml==0.10.1 pyarrow==1.0.0
```

Whenever rust code changes (your changes or via git pull):

```bash
source venv/bin/activate
maturin develop
python -m unittest discover tests
```
