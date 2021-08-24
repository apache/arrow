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

Apache Arrow Datasets API for Go
================================

The [Apache Arrow][arrow] Dataset API is experimental and subject to change
as the development is performed in order to expose a Go-compatible API to the
Tabular Dataset API which is available in C++ and Python. In contrast to the
Arrow and Parquet Golang packages, this package is not pure Go and utilizes
cgo in order to tap into the C++ Arrow Dataset package rather than reimplement
it. As such, use of this package requires installation of the headers and 
compiled binaries of the arrow-dataset library and its dependencies.

[arrow]:    https://arrow.apache.org
