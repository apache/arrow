---
layout: default
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

## Powered By

### Project and Product names using "Apache Arrow"

Organizations creating products and projects for use with Apache Arrow, along
with associated marketing materials, should take care to respect the trademark
in "Apache Arrow" and its logo. Please refer to [ASF Trademarks Guidance][1]
and associated [FAQ][2] for comprehensive and authoritative guidance on proper
usage of ASF trademarks.

Names that do not include "Apache Arrow" at all have no potential trademark
issue with the Apache Arrow project. This is recommended.

Names like "Apache Arrow BigCoProduct" are not OK, as are names including
"Apache Arrow" in general. The above links, however, describe some exceptions,
like for names such as "BigCoProduct, powered by Apache Arrow" or
"BigCoProduct for Apache Arrow".

It is common practice to create software identifiers (Maven coordinates, module
names, etc.) like "arrow-foo". These are permitted. Nominative use of trademarks
in descriptions is also always allowed, as in "BigCoProduct is a widget for
Apache Arrow".

### OpenSource Projects

To add yourself to the list, please email dev@arrow.apache.org with your
organization name, URL, a list of which Arrow components you are using, and a
short description of your use case.

 * **[Apache Parquet][3]:** A columnar storage format available to any project
   in the Hadoop ecosystem, regardless of the choice of data processing
   framework, data model or programming language. The C++ and Java
   implementation provide vectorized reads and write to/from Arrow data
   structures.
 * **[Apache Spark][7]:** Apache Spark™ is a fast and general engine for
   large-scale data processing. Spark uses Apache Arrow to
    1. improve performance of conversion between Spark DataFrame and pandas DataFrame
    2. enable a set of vectorized user-defined functions (`pandas_udf`) in PySpark.
 * **[GeoMesa][8]:** A suite of tools that enables large-scale geospatial query
    and analytics on distributed computing systems. GeoMesa supports query
    results in the Arrow IPC format, which can then be used for in-browser
    visualizations and/or further analytics.
 * **[Ray][5]:** A flexible, high-performance distributed execution framework
   with a focus on machine learning and AI applications. Uses Arrow to
   efficiently store Python data structures containing large arrays of
   numerical data. Data can be accessed with zero-copy by multiple processes
   using the [Plasma shared memory object store][6] which originated from Ray and
   is part of Arrow now.
 * **[Turbodbc][4]:** Python module to access relational databases via the Open
   Database Connectivity (ODBC) interface. It provides the ability to return Arrow
   Tables and RecordBatches in addition to the Python Database API Specification
   2.0.

### Companies and Organizations

To add yourself to the list, please email dev@arrow.apache.org with your
organization name, URL, a list of which Arrow components you are using, and a
short description of your use case.

  * **[Dremio][9]:** A self-service data platform. Dremio makes it easy for
    users to discover, curate, accelerate, and share data from any source.
    It includes a distributed SQL execution engine based on Apache Arrow.
    Dremio reads data from any source (RDBMS, HDFS, S3, NoSQL) into Arrow
    buffers, and provides fast SQL access via ODBC, JDBC, and REST for BI,
    Python, R, and more (all backed by Apache Arrow).


[1]: https://www.apache.org/foundation/marks/
[2]: https://www.apache.org/foundation/marks/faq/
[3]: https://parquet.apache.org/
[4]: https://github.com/blue-yonder/turbodbc
[5]: https://github.com/ray-project/ray
[6]: https://ray-project.github.io/2017/08/08/plasma-in-memory-object-store.html
[7]: https://spark.apache.org/
[8]: https://github.com/locationtech/geomesa
[9]: https://www.dremio.com/
