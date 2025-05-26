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

<!--
This is the source for the async.svg diagram used in developer_guide.rst
-->
sequenceDiagram
    %% This is the source diagram for async.svg
    %% included here for future doc maintainers
    Thread Pool->>+CPU: Start Task
    CPU->>+IO: Read
    IO->>+CPU: Future<Buffer>
    activate IO
    CPU->>+CPU: Add Continuation
    CPU->>+Thread Pool: Finish Task
    Note right of IO: Blocked on IO
    Thread Pool->>+CPU: Other Task
    CPU->>+Thread Pool:
    Thread Pool->>+CPU: Other Task
    CPU->>+Thread Pool:
    Thread Pool->>+CPU: Other Task
    CPU->>+Thread Pool:
    deactivate IO
    IO->>+IO: Read Finished
    IO->>+IO: Run Continuation
    IO->>+Thread Pool: Schedule Task
    Thread Pool->>+CPU: Start Task
    CPU->>+CPU: Process Read Result
