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

# Arrow Java API Changes

This document tracks behavior changes to Java APIs, as listed below.

- **[ARROW-5973](https://issues.apache.org/jira/browse/ARROW-5973)**:
  * **Start date**: 2019/07/18
  * **Resolve date**: 2019/07/20
  * **Brief description**: The semantics of the get methods for [VarCharVector](./vector/scr/main/org/apache/arrow/vector/VarCharVector.java), [VarBinaryVector](./vector/scr/main/org/apache/arrow/vector/VarBinaryVector.java), and [FixedSizeBinaryVector](./vector/scr/main/org/apache/arrow/vector/FixedSizeBinaryVector.java) changes. In the past, if the validity bit is clear, the methods throw throws an IllegalStateException when NULL_CHECKING_ENABLED is set, or returns an empty object when the flag is not set. Now, the get methods return a null if the validity bit is clear.

- **[ARROW-5842](https://issues.apache.org/jira/browse/ARROW-5842)**:
  * **Start date**: 2019/07/04
  * **Resolve date**: 2019/07/11
  * **Brief description**: The semantics of lastSet member in class [ListVector](./vector/src/main/java/org/apache/arrow/vector/complex/ListVector.java) changes. In the past, it refers to the next index that will be set. After the change it points to the last index that is actually set.
