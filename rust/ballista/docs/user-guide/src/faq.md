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
# Frequently Asked Questions

## What is the relationship between Apache Arrow, DataFusion, and Ballista?

Apache Arrow is a library which provides a standardized memory representation for columnar data. It also provides
"kernels" for performing common operations on this data.

DataFusion is a library for executing queries in-process using the Apache Arrow memory 
model and computational kernels. It is designed to run within a single process, using threads 
for parallel query execution. 

Ballista is a distributed compute platform design to leverage DataFusion and other query
execution libraries.