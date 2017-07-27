---
layout: post
title: "Speeding up PySpark with Apache Arrow"
date: "2017-07-26 08:00:00 -0800"
author: BryanCutler
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

*[Bryan Cutler][11] is a software engineer at IBM's Spark Technology Center [STC][12]*

Beginning with [Apache Spark][1] version 2.3, [Apache Arrow][2] will be a supported
dependency and begin to offer increased performance with columnar data transfer.
If you are a Spark user that prefers to work in Python and Pandas, this is a cause
to be excited over! The initial work is limited to collecting a Spark DataFrame
with `toPandas()`, which I will discuss below, however there are many additional
improvements that are currently [underway][3].

# Optimizing Spark Conversion to Pandas

The previous way of converting a Spark DataFrame to Pandas with `DataFrame.toPandas()`
in PySpark was painfully inefficient. Basically, it worked by first collecting all
rows to the Spark driver. Next, each row would get serialized into Python's pickle
format and sent to a Python worker process. This child process unpickles each row into
a huge list of tuples. Finally, a Pandas DataFrame is created from the list using
`pandas.DataFrame.from_records()`.

This all might seem like standard procedure, but suffers from 2 glaring issues: 1)
even using CPickle, Python serialization is a slow process and 2) creating
a `pandas.DataFrame` using `from_records` must slowly iterate over the list of pure
Python data and convert each value to Pandas format. See [here][4] for a detailed
analysis.

Here is where Arrow really shines to help optimize these steps: 1) Once the data is
in Arrow memory format, there is no need to serialize/pickle anymore as Arrow data can
be sent directly to the Python process, 2) When the Arrow data is received in Python,
then pyarrow can utilize zero-copy methods to create a `pandas.DataFrame` from entire
chunks of data at once instead of processing individual scalar values. Additionally,
the conversion to Arrow data can be done on the JVM and pushed back for the Spark
executors to perform in parallel, drastically reducing the load on the driver.

As of the merging of [SPARK-13534][5], the use of Arrow when calling `toPandas()`
needs to be enabled by setting the SQLConf "spark.sql.execution.arrow.enable" to
"true".  Let's look at a simple usage example.

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.0-SNAPSHOT
      /_/

Using Python version 2.7.13 (default, Dec 20 2016 23:09:15)
SparkSession available as 'spark'.

In [1]: from pyspark.sql.functions import rand
   ...: df = spark.range(1 << 22).toDF("id").withColumn("x", rand())
   ...: df.printSchema()
   ...: 
root
 |-- id: long (nullable = false)
 |-- x: double (nullable = false)


In [2]: %time pdf = df.toPandas()
CPU times: user 17.4 s, sys: 792 ms, total: 18.1 s
Wall time: 20.7 s

In [3]: spark.conf.set("spark.sql.execution.arrow.enable", "true")

In [4]: %time pdf = df.toPandas()
CPU times: user 40 ms, sys: 32 ms, total: 72 ms                                 
Wall time: 737 ms

In [5]: pdf.describe()
Out[5]: 
                 id             x
count  4.194304e+06  4.194304e+06
mean   2.097152e+06  4.998996e-01
std    1.210791e+06  2.887247e-01
min    0.000000e+00  8.291929e-07
25%    1.048576e+06  2.498116e-01
50%    2.097152e+06  4.999210e-01
75%    3.145727e+06  7.498380e-01
max    4.194303e+06  9.999996e-01
```

This example was run locally on my laptop using Spark defaults so the times
shown should not be taken precisely. Even though, it is clear there is a huge
performance boost and using Arrow took something that was excruciatingly slow
and speeds it up to be barely noticeable.

# Notes on Usage

Here are some things to keep in mind before making use of this new feature. At
the time of writing this, pyarrow will not be installed automatically with
pyspark and needs to be manually installed, see installation [instructions][6].
It is planned to add pyarrow as a pyspark dependency so that 
`> pip install pyspark` will also install pyarrow.

Currently, the controlling SQLConf is disabled by default. This can be enabled
programmatically as in the example above or by adding the line
"spark.sql.execution.arrow.enable=true" to `SPARK_HOME/conf/spark-defaults.conf`.

Also, not all Spark data types are currently supported and limited to primitive
types. Expanded type support is in the works and expected to also be in the Spark
2.3 release.

# Future Improvements

As mentioned, this was just a first step in using Arrow to make life easier for
Spark Python users. A few exciting initiatives in the works are to allow for
vectorized UDF evaluation ([SPARK-21190][7], [SPARK-21404][8]), and the ability
to apply a function on grouped data using a Pandas DataFrame ([SPARK-20396][9]).
Just as Arrow helped in converting a Spark to Pandas, it can also work in the
other direction when creating a Spark DataFrame from an existing Pandas
DataFrame ([SPARK-20791][10]). Stay tuned for more!

# Collaborators

Reaching this first milestone was a group effort from both the Apache Arrow and
Spark communities. Thanks to the hard work of [Wes McKinney][13], [Li Jin][14],
[Holden Karau][15], Reynold Xin, Wenchen Fan, Shane Knapp and many others that
helped push this effort forwards.

[1]: https://spark.apache.org/
[2]: https://arrow.apache.org/
[3]: https://issues.apache.org/jira/issues/?filter=12335725&jql=project%20%3D%20SPARK%20AND%20status%20in%20(Open%2C%20%22In%20Progress%22%2C%20Reopened)%20AND%20text%20~%20%22arrow%22%20ORDER%20BY%20createdDate%20DESC
[4]: https://gist.github.com/wesm/0cb5531b1c2e346a0007
[5]: https://issues.apache.org/jira/browse/SPARK-13534
[6]: https://github.com/apache/arrow/blob/master/site/install.md
[7]: https://issues.apache.org/jira/browse/SPARK-21190
[8]: https://issues.apache.org/jira/browse/SPARK-21404
[9]: https://issues.apache.org/jira/browse/SPARK-20396
[10]: https://issues.apache.org/jira/browse/SPARK-20791
[11]: https://github.com/BryanCutler
[12]: http://www.spark.tc/
[13]: https://github.com/wesm
[14]: https://github.com/icexelloss
[15]: https://github.com/holdenk
