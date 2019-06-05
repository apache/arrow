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

# Arrow C++ Datasets

The `arrow::dataset` subcomponent provides an API to read and write
semantic datasets stored in different locations and formats. It
facilitates parallel processing of datasets spread across different
physical files and serialization formats. Other concerns such as
partitioning, filtering (partition- and column-level), and schema
normalization are also addressed.

## Development Status

Pre-alpha as of June 2019. API subject to change without notice.