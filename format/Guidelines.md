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
# Implementation guidelines

An execution engine (or framework, or UDF executor, or storage engine, etc) can implements only a subset of the Arrow spec and/or extend it given the following constraints:

## Implementing a subset the spec
### If only producing (and not consuming) arrow vectors.
Any subset of the vector spec and the corresponding metadata can be implemented.

### If consuming and producing vectors
There is a minimal subset of vectors to be supported.
Production of a subset of vectors and their corresponding metadata is always fine.
Consumption of vectors should at least convert the unsupported input vectors to the supported subset (for example Timestamp.millis to timestamp.micros or int32 to int64)

## Extensibility
An execution engine implementor can also extend their memory representation with their own vectors internally as long as they are never exposed. Before sending data to another system expecting Arrow data these custom vectors should be converted to a type that exist in the Arrow spec.
An example of this is operating on compressed data.
These custom vectors are not exchanged externally and there is no support for custom metadata.
