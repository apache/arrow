---
layout: post
title: "Connecting Relational Databases to the Apache Arrow World with turbodbc"
date: "2017-06-16 04:00:00 -0400"
author: MathMagique
categories: [application]
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

*[Michael König][15] is the lead developer of the [turbodbc project][2]*

The [Apache Arrow][1] project set out to become the universal data layer for
column-oriented data processing systems without incurring serialization costs
or compromising on performance on a more general level. While relational
databases still lag behind in Apache Arrow adoption, the Python database module
[turbodbc][2] brings Apache Arrow support to these databases using a much
older, more specialized data exchange layer: [ODBC][3].

ODBC is a database interface that offers developers the option to transfer data
either in row-wise or column-wise fashion. Previous Python ODBC modules typically
use the row-wise approach, and often trade repeated database roundtrips for simplified
buffer handling. This makes them less suited for data-intensive applications,
particularly when interfacing with modern columnar analytical databases.

In contrast, turbodbc was designed to leverage columnar data processing from day
one. Naturally, this implies using the columnar portion of the ODBC API. Equally
important, however, is to find new ways of providing columnar data to Python users
that exceed the capabilities of the row-wise API mandated by Python’s [PEP 249][4].
Turbodbc has adopted Apache Arrow for this very task with the recently released
version 2.0.0:

```python
>>> from turbodbc import connect
>>> connection = connect(dsn="My columnar database")
>>> cursor = connection.cursor()
>>> cursor.execute("SELECT some_integers, some_strings FROM my_table")
>>> cursor.fetchallarrow()
pyarrow.Table
some_integers: int64
some_strings: string
```

With this new addition, the data flow for a result set of a typical SELECT query
is like this:
*   The database prepares the result set and exposes it to the ODBC driver using
    either row-wise or column-wise storage.
*   Turbodbc has the ODBC driver write chunks of the result set into columnar buffers.
*   These buffers are exposed to turbodbc's Apache Arrow frontend. This frontend
    will create an Arrow table and fill in the buffered values.
*   The previous steps are repeated until the entire result set is retrieved.

![Data flow from relational databases to Python with turbodbc and the Apache Arrow frontend]({{ site.url }}/img/turbodbc_arrow.png){:class="img-responsive" width="75%"}

In practice, it is possible to achieve the following ideal situation: A 64-bit integer
column is stored as one contiguous block of memory in a columnar database. A huge chunk
of 64-bit integers is transferred over the network and the ODBC driver directly writes
it to a turbodbc buffer of 64-bit integers. The Arrow frontend accumulates these values
by copying the entire 64-bit buffer into a free portion of an Arrow table's 64-bit
integer column.

Moving data from the database to an Arrow table and, thus, providing it to the Python
user can be as simple as copying memory blocks around, megabytes equivalent to hundred
thousands of rows at a time. The absence of serialization and conversion logic renders
the process extremely efficient.

Once the data is stored in an Arrow table, Python users can continue to do some
actual work. They can convert it into a [Pandas DataFrame][5] for data analysis
(using a quick `table.to_pandas()`), pass it on to other data processing
systems such as [Apache Spark][6] or [Apache Impala (incubating)][7], or store
it in the [Apache Parquet][8] file format. This way, non-Python systems are
efficiently connected with relational databases.

In the future, turbodbc’s Arrow support will be extended to use more
sophisticated features such as [dictionary-encoded][9] string fields. We also
plan to pick smaller than 64-bit [data types][10] where possible. Last but not
least, Arrow support will be extended to cover the reverse direction of data
flow, so that Python users can quickly insert Arrow tables into relational
databases.

If you would like to learn more about turbodbc, check out the [GitHub project][2] and the
[project documentation][11]. If you want to learn more about how turbodbc implements the
nitty-gritty details, check out parts [one][12] and [two][13] of the
["Making of turbodbc"][12] series at [Blue Yonder's technology blog][14].


[1]: https://arrow.apache.org/
[2]: https://github.com/blue-yonder/turbodbc
[3]: https://en.wikipedia.org/wiki/Open_Database_Connectivity
[4]: https://www.python.org/dev/peps/pep-0249/
[5]: https://arrow.apache.org/docs/python/pandas.html
[6]: http://spark.apache.org/
[7]: http://impala.apache.org/
[8]: http://parquet.apache.org/
[9]: https://arrow.apache.org/docs/memory_layout.html#dictionary-encoding
[10]: https://arrow.apache.org/docs/metadata.html#integers
[11]: http://turbodbc.readthedocs.io/
[12]: https://tech.blue-yonder.com/making-of-turbodbc-part-1-wrestling-with-the-side-effects-of-a-c-api/
[13]: https://tech.blue-yonder.com/making-of-turbodbc-part-2-c-to-python/
[14]: https://tech.blue-yonder.com/
[15]: https://github.com/mathmagique