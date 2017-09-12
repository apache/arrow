.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. currentmodule:: pyarrow
.. _other_systems:

Using Arrow with other systems
==============================

Timestamps
----------

Timestamps are data structures that mark a particular point in time
and we want to be able to order them, regardless of where
they originated.
For human consumption timestamps are usually specified by the date
together with the time of day, often using the local time zone.
The problem with this scheme is that if things need to be ordered
by time across multiple time zones, using local time can be ambiguous.
Therefore timestamps from multiple time zones should always be collected,
stored and communicated in UTC to avoid this ambiguity.

Most computer systems do not store timestamps as two part values
with date part and time within that date, as most of us humans
think about them. Instead the timestamp is stored as a single value
offset from a given point in time in some time units, i.e. seconds,
milliseconds, etc. An example of this is the Unix timestamp which is
the number of seconds since midnight January 1st, 1970 in the UTC
time zone. When the timestamp is then presented to an end user
the scalar value is converted to the familiar date time format.

Note the importance of the time zone in the conversion from scalar
timestamp value to date and time. The Unix timestamp value 0 is
translated to '1969-12-31 20:00:00' in the 'America/New_York' time
zone, because it is defined in UTC, and New York was four hours
behind UTC at that point in time. Systems that do use the
local time zone of the server as reference for calculating the
timestamp offset value can cause problems when those values need to
be communicated to other systems. 

Timestamps from systems described above are called `non-UTC-normalized`.
Arrow, on the other hand, does always use UTC as the base for calculating
timestamp offsets as further described below. Timestamps in Arrow are called
`UTC-normalized`. Special care must always be taken when data from a
system that that is `non-UTC-normalized` is read by Arrow.

Timestamp types in Arrow are specified with a resolution and optional
time zone. Several utility functions exist to convert data from 
Pandas to Arrow, including functions that convert timestamp values 
to milliseconds, because Pandas uses nanoseconds and other systems,
i.e. Parquet use timestamps in milliseconds.

In Arrow, timestamps have two forms, depending if the time zone is
specified or not:

*   **Time zone naive** (where ``tz=None`` in Python); there is no 
    notion of UTC or local time zone. Python will interpret a 
    timestamp like ``2017-09-12 12:00:00`` to be in the local time
    zone, not UTC. In Arrow on the other hand the timestamp shall
    be displayed as is to the user and not localized to their
    time zone. The value of the timestamp will be treated is if
    it was specified in UTC.

*   **Time zone aware** where the integer values are internally 
    normalized to UTC. This means that the underlying timestamp
    always has a value relative to UTC, enabling direct comparison
    of two timestamps.

Apache Spark
++++++++++++

When using Arrow with `Apache Spark <http://spark.apache.org/>`_
special attention must be given to conversion of timestamps.

Spark, unlike Arrow, does treat timestamp data without a time zone
as being in the local time zone of the server running the code.
Spark version 2.2 allows for setting a configuration variable:
``spark.sql.session.timeZone`` to ``UTC``. If not set this variable
defaults to system local time zone. The configuration
variable does only work on timestamps generated with ``SparkSQL``.
In order to force timestamps coming from other sources, i.e. 
external files, to be interpreted as UTC, execute the following
on all cluster nodes before the data is loaded into PySpark
to set the system timezone::

    os.environ["TZ"] = "UTC"
    time.tzset()

Also be careful when using the ``from_utc_timestamp`` function in Spark,
as it returns a simple ``datetime`` object that does not contain any
time zone information, and only shifts the time value based on the
time zone and date specified. In the following case the shift is 6
hours which is the offset Denver is behind UTC during 
Daylight Saving Time::

    In [9]: from pyspark.sql.functions import from_utc_timestamp

    In [10]: from datetime import datetime

    In [11]: df = sqlContext.createDataFrame([('2017-09-12 22:30:00',)], ['t'])

    In [12]: df.select(from_utc_timestamp(df.t, "America/Denver").alias('t')).collect()
    Out[12]: [Row(t=datetime.datetime(2017, 9, 12, 16, 30))]

    In [13]: datetime.strptime("2017-09-12 22:30:00", "%Y-%m-%d %H:%M:%S").timestamp()
    Out[13]: 1505277000.0

    In [14]: datetime(2017, 9, 12, 16, 30).timestamp()
    Out[14]: 1505255400.0

The difference in the timestamps above is 21,600 seconds, or 6 hours as expected.

The `Pandas Library <http://pandas.pydata.org/>`_ on the other hand has
full support for timestamps using time zones, as can be seen by the
following example::

    In [16]: import pandas as pa

    In [17]: val = pa.Timestamp('1970-01-01 00:00:00+0000', tz='UTC')

    In [18]: val
    Out[18]: Timestamp('1970-01-01 00:00:00+0000', tz='UTC')

    In [19]: val.value
    Out[19]: 0

Here the timestamp value, ``val.value`` is the offset from the Unix epoch,
which reference is midnight of January 1, 1970 UTC.

Timestamps can be converted between time zones and the new timestamp
stores the new zone::

    In [24]: val2 = val.tz_convert('America/Denver')

    In [25]: val2
    Out[25]: Timestamp('1969-12-31 17:00:00-0700', tz='America/Denver')

    In [26]: val2.value
    Out[26]: 0

Here the previous ``val`` is converted to the Denver time zone correctly
showing the local time in Denver as 5pm on New Years Eve at midnight UTC.
Also note that the underlying ``value`` has not changed as it refers
to the same point in time.

Apache Parquet
++++++++++++++

When using Arrow with `Apache Parquet <http://parquet.apache.org/>`_
the following issues should also be considered.

Parquet currently uses an eight byte integer INT64 to store timestamps
but there exists a now deprecated INT96 timestamp type that uses 12
bytes. Parquet files written by Arrow can optionally write the old
format by setting the ``use_deprecated_int96_timestamps`` option to
``True``. If the Parquet files are to be read by Spark the writing
should be done with the ``spark`` option (introduced in Arrow 0.7)::

    pq.write_table(..., flavor='spark')

This option provides compatibility with Spark, which has not completely
made the change from INT96.

Timestamps in Parquet are stored either as number of milliseconds
form the Unix epoch (1970-01-01 00:00:00.000 UTC), called
``TIMESTAMP_MILLIS``, or in microseconds from the epoch, called
``TIMESTAMP_MICROS``. Using UTC as reference should avoid any
confusion in the interpretation of their values.
