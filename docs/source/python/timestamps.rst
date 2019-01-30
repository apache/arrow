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
Timestamps
==========

Arrow/Pandas Timestamps
-----------------------

The Arrow timestamp is stored as a numeric value (64-bit integer) with metadata
of a time unit (e.g. seconds or microseconds), and an optional timezone
assocated with a column.  Python (datetime) and Pandas (Timestamp) have
essentially the same representations as Arrow.  Pandas's default time unit is
nanoseconds.  Python/Pandas timestamp types without a associated timezone are
referred to as "Naive".  Python/Pandas timestamp types with an associated
timezone are referred to "Aware".   


Timestamp Conversions
---------------------

Pandas/Arrow ⇄ Spark
~~~~~~~~~~~~~~~~~~~~

Spark stores its timestamps as 64-bit integers representing microseconds since
the UNIX epoch.  It does not store any metadata about timezones with its
timestamps.  

Spark interprets timestamps by default with the *session local timezone*,
``spark.sql.session.timeZone``. If that timezone is undefined, Spark turns to
the default system timezone. For simplicity's sake below, assume the session
local timezone is always defined.

This implies a few things when round-tripping timestamps:
1.  Timezone information is lost.
2.  Data is truncated to microseconds.
3.  Changing the session time-zone between loading a pandas data
    frame into spark and retreiving that dataframe from spark
    can cause changes to timestamp values.

** Converting Arrow To Spark **

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
    >>> utc_df = sqlContext.createDataFrame(pdf).
    >>> utf_df.show()
    +-------------------+-------------------+
    |              naive|              aware|
    +-------------------+-------------------+
    |2019-01-01 00:00:00|2019-01-01 08:00:00|
    +-------------------+-------------------+
                    
Note that conversion of the aware timezone is shifted to reflect the time
assuming UTC. 

** Spark to Pandas **

Now if the the session timezone to US Pacific Time (PST):

::

    >>> spark.conf.set("spark.sql.session.timeZone", "US/Pacific")
    >>> pst_df = sqlContext.createDataFrame(pdf)
    >>> pst_df.show()
    +-------------------+-------------------+
    |              naive|              aware|
    +-------------------+-------------------+
    |2019-01-01 00:00:00|2019-01-01 00:00:00|
    +-------------------+-------------------+

But if we look again at utc_df.show() we see one of the tricky aspects.
Because the naive timestamp was initially converted assuming the UTC timezone,
the display value is shifted back to represent the correct instant in time in
PST.

::
    >>> utc_df.show()
    +-------------------+-------------------+
    |              naive|              aware|
    +-------------------+-------------------+
    |2018-12-31 16:00:00|2019-01-01 00:00:00|
    +-------------------+-------------------+


We can observe what happens when converting back to Arrow/Pandas (this is the
suprising part).  Assuming the session timezone is still PST:

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
    
This is a big gotcha.  In addition to no longer having an associated timezone,
the 'aware' value is now a different instant in
time (2019-01-01 GMT).  Specifically, it is 8 hours before the original time. 

::

  >>> (pst_df.toPandas()['aware'][0].timestamp()-pdf['aware'][0].timestamp())/3600
  -8.0

The same type of conversion happens with the dataframe converted while 
the session time-zone wast UTC.  In this case both naive and aware 
represent different instants in time (the naive instant is due to 
the change in session timezone while creating dataframes).

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

Note that the suprising shift in for aware doesn't happen
when the session timezone is UTC.
  
::
  
  >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
  >>> pst_df.show()
  +-------------------+-------------------+
  |              naive|              aware|
  +-------------------+-------------------+
  |2019-01-01 08:00:00|2019-01-01 08:00:00|
  +-------------------+-------------------+
  
  >>> (pst_df.toPandas()['aware'][0].timestamp()-pdf['aware'][0].timestamp())/3600
  0.0
