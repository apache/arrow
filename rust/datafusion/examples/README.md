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

# DataFusion Examples

## Single Process

The examples `csv_sql.rs` and `parquet_sql.rs` demonstrate building a query plan from a SQL statement and then executing the query plan against local CSV and Parquet files, respectively.

## Distributed

The `flight-client.rs` and `flight-server.rs` examples demonstrate how to run DataFusion as a standalone process and execute SQL queries from a client using the Flight protocol.