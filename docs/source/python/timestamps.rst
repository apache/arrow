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

**********
Timestamps
**********

Arrow/Pandas Timestamps
=======================

Arrow timestamps are stored as a 64-bit integer with column metadata to
associate a time unit (e.g. milliseconds, microseconds, or nanoseconds), and an
optional time zone.  Pandas (`Timestamp`) uses a 64-bit integer represesenting
nanoseconds and an optional time zone.
Python/Pandas timestamp types without a associated time zone are referred to as
"Time Zone Naive".  Python/Pandas timestamp types with an associated time zone are
referred to as "Time Zone Aware".


Timestamp Conversions
=====================

Pandas/Arrow ⇄ Spark
--------------------

Spark stores timestamps as 64-bit integers representing microseconds since
the UNIX epoch.  It does not store any metadata about time zones with its
timestamps.

Spark interprets timestamps with the *session local time zone*, (i.e.
``spark.sql.session.timeZone``). If that time zone is undefined, Spark turns to
the default system time zone. For simplicity's sake below, the session
local time zone is always defined.

This implies a few things when round-tripping timestamps:

#.  Timezone information is lost (all timestamps that result from
    converting from spark to arrow/pandas are "time zone naive").
#.  Timestamps are truncated to microseconds.
#.  The session time zone might have unintuitive impacts on 
    translation of timestamp values. 

Spark to Pandas (through Apache Arrow)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following cases assume the Spark configuration
``spark.sql.execution.arrow.enabled`` is set to ``"true"``.

::

    >>> pdf = pd.DataFrame({'naive': [datetime(2019, 1, 1, 0)], 
    ...                     'aware': [Timestamp(year=2019, month=1, day=1, 
    ...                               nanosecond=500, tz=timezone(timedelta(hours=-8)))]})
    >>> pdf
           naive                               aware
           0 2018-10-01 2018-10-01 00:00:00.000000500-08:00

    >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
    >>> utc_df = sqlContext.createDataFrame(pdf)
    >>> utf_df.show()
    +-------------------+-------------------+
    |              naive|              aware|
    +-------------------+-------------------+
    |2019-01-01 00:00:00|2019-01-01 08:00:00|
    +-------------------+-------------------+
                    
Note that conversion of the aware timestamp is shifted to reflect the time
assuming UTC (it represents the same instant in time).  For naive
timestamps, Spark treats them as being in the system local
time zone and converts them UTC. Recall that internally, the schema
for spark dataframe's does not store any time zone information with
timestamps.

Now if the session time zone is set to US Pacific Time (PST) we don't
see any shift in the display of the aware time zone (it
still represents the same instant in time):

::

    >>> spark.conf.set("spark.sql.session.timeZone", "US/Pacific")
    >>> pst_df = sqlContext.createDataFrame(pdf)
    >>> pst_df.show()
    +-------------------+-------------------+
    |              naive|              aware|
    +-------------------+-------------------+
    |2019-01-01 00:00:00|2019-01-01 00:00:00|
    +-------------------+-------------------+

Looking again at utc_df.show() we see one of the impacts of the session time
zone.  The naive timestamp was initially converted assuming UTC, the instant it
reflects is actually earlier than the naive time zone from the PST converted
data frame:

::

    >>> utc_df.show()
    +-------------------+-------------------+
    |              naive|              aware|
    +-------------------+-------------------+
    |2018-12-31 16:00:00|2019-01-01 00:00:00|
    +-------------------+-------------------+

Spark to Pandas
~~~~~~~~~~~~~~~

We can observe what happens when converting back to Arrow/Pandas.  Assuming the
session time zone is still PST:

::

   >>> pst_df.show()
   +-------------------+-------------------+
   |              naive|              aware|
   +-------------------+-------------------+
   |2019-01-01 00:00:00|2019-01-01 00:00:00|
   +-------------------+-------------------+

   
    >>> pst_df.toPandas()
    naive      aware
    0 2019-01-01 2019-01-01
    >>> pst_df.toPandas().info()
    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 1 entries, 0 to 0
    Data columns (total 2 columns):
    naive    1 non-null datetime64[ns]
    aware    1 non-null datetime64[ns]
    dtypes: datetime64[ns](2)
    memory usage: 96.0 bytes
    
Notice that, in addition to being a "time zone naive" timestamp, the 'aware'
value will now differ when converting to an epoch offset.  Spark does the conversion
by first converting to the session time zone (or system local time zone if
session time zones isn't set) and then localizes to remove the time zone
information.  This results in the timestamp being 8 hours before the original
time:

::

  >>> pst_df.toPandas()['aware'][0]
  Timestamp('2019-01-01 00:00:00')
  >>> pdf['aware'][0]
  Timestamp('2019-01-01 00:00:00.000000500-0800', tz='UTC-08:00')
  >>> (pst_df.toPandas()['aware'][0].timestamp()-pdf['aware'][0].timestamp())/3600
  -8.0

The same type of conversion happens with the data frame converted while 
the session time zone was UTC.  In this case both naive and aware 
represent different instants in time (the naive instant is due to 
the change in session time zone between creating data frames):

::

  >>> utc_df.show()
  +-------------------+-------------------+
  |              naive|              aware|
  +-------------------+-------------------+
  |2018-12-31 16:00:00|2019-01-01 00:00:00|
  +-------------------+-------------------+

  >>> utc_df.toPandas()
  naive      aware
  0 2018-12-31 16:00:00 2019-01-01

Note that the surprising shift for aware doesn't happen
when the session time zone is UTC (but the timestamps
still become "time zone naive"):
  
::
  
  >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
  >>> pst_df.show()
  +-------------------+-------------------+
  |              naive|              aware|
  +-------------------+-------------------+
  |2019-01-01 08:00:00|2019-01-01 08:00:00|
  +-------------------+-------------------+
  
  >>> pst_df.toPandas()['aware'][0]
  Timestamp('2019-01-01 08:00:00')
  >>> pdf['aware'][0]
  Timestamp('2019-01-01 00:00:00.000000500-0800', tz='UTC-08:00')
  >>> (pst_df.toPandas()['aware'][0].timestamp()-pdf['aware'][0].timestamp())/3600
  0.0
