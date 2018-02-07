All About Timestamps (work in progress)
=======================================

**Table of Contents**
---------------------

**`Definitions <#definitions>`__ \| `Kinds of Timestamp
Object <#kinds-of-timestamp-object>`__ \| `Timestamps in Different
Systems <#timestamps-in-different-systems>`__ \| `Timestamp
Conversions <#timestamp-conversions-arrow--spark>`__**

--------------

Definitions
-----------

#. **Timestamp**. An object modeling the time, and usually including a
   string representation. The international standard for timestamps is
   `ISO
   8601 <https://www.iso.org/iso-8601-date-and-time-format.html>`__.

#. **Timezone**. A geographic region that marks time in some
   standardized way, and defined relative to other timezones. Examples
   of timezones include UTC (Coordinated Universal Time, historically
   known as Greenwich Mean Time) and "America/New York". Timezones are
   conventionally represented as *offsets* from UTC. Timezone is often
   seen abbreviated *tz* in programming contexts.

#. **Instant Value**. The exact point a timestamp defines on some
   timeline. For instance:

``1970-01-01 00:00:00 UTC``

has the same instant value as:

``1969-12-31 19:00:00 America/New_York``

Even though the two examples are in distinct timezones, and actually
even in different calendar years, they correspond to the same absolute
point in time; that point is the *instant value* of each.

#. **Interpretation**. Internally, A timestamp is usually represented as
   a number. *Interpretation* means how a program parses that number and
   represents it or certain elements of it. The most common
   interpretation of a timestamp is its direct representation as a
   whole, human-readable string. But interpretation can also mean
   combining the string with separate timezone information. It can also
   include the result of some function on a timestamp: for instance,
   returning only the hour-portion of it.

--------------

Kinds of Timestamp Object
-------------------------

Timestamps are normally objects, and there are three different kinds:

-  `timestamps with an explicit
   timezone <#timestamps-with-an-explicit-time-zone>`__
-  `timestamps with an implicit
   timezone <#timestamps-with-an-implicit-time-zone>`__
-  `tz-naive timestamps <#timezone-naive-timestamps>`__

We discuss these below, in turn.

Timestamps with an Explicit Timezone
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This kind of timestamp defines a single, instantaneous point on the
timeline, and the timezone is defined explicitly as part of the
timestamp. For instance, ``UTC`` is the explicit timezone in:

::

    1970-01-01 00:00:00 UTC

A timestamp with an explicit timezone has both an instant value and an
interpretation. Examples include:

-  The Joda/Java8
   ```ZonedDateTime`` <https://js-joda.github.io/js-joda/esdoc/class/src/ZonedDateTime.js~ZonedDateTime.html>`__
   type.

-  A Python/Pandas timestamp of an *aware* ``datetime`` or ``time``
   object. Loosely speaking, an aware object contains timezone
   information and a naive one doesn't. The `Python docs for
   ``datetime.timezone`` <https://docs.python.org/3/library/datetime.html#available-types>`__
   offer a more technical definition:

    An object of type ``time`` or ``datetime`` may be naive or aware. A
    ``datetime`` object *d* is aware if ``d.tzinfo`` is not ``None`` and
    ``d.tzinfo.utcoffset(d)`` does not return ``None``. If ``d.tzinfo``
    is ``None``, or if ``d.tzinfo`` is not ``None`` but
    ``d.tzinfo.utcoffset(d)`` returns ``None``, *d* is naive. A time
    object *t* is aware if ``t.tzinfo`` is not ``None`` and
    ``t.tzinfo.utcoffset(None)`` does not return ``None``. Otherwise,
    *t* is naive.

Timestamps with an Implicit Timezone
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This kind of timestamp defines an instant value but not an
interpretation. Its interpretation relies on information external to
itself. For instance, some systems turn to the user's local timezone, or
a timezone configured by the user, to interpret the timestamp.
Timestamps whose timezone is implicit have the following format:

::

    1970-01-01 00:00:00

The interpretation of this kind of timestamp object depends by default
on the user's local or session timezone. For instance, consider a
timestamp that explicitly represents one point in time:

::

    1970-01-01 00:00:00 UTC

If the user's local timezone is UTC, interpreting this timestamp
returns:

::

    1970-01-01 00:00:00

If the user's local timezone is New York, interpreting the timestamp
instead returns:

::

    1969-12-31 19:00:00

These two interpretations represent the same instantaneous point on an
absolute timeline, but note that the timezone is not represented in
either timestamp; the timezone is implicit — hence the name.

Examples of this kind of timestamp include:

-  Parquet
   ```TIMESTAMP_MILLIS`` <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp_millis>`__
   and
   ```TIMESTAMP_MICROS`` <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp_micros>`__
   logical types.
-  Spark SQL
   ```TimestampType`` <https://spark.apache.org/docs/latest/sql-programming-guide.html#data-types>`__.

Timezone-Naive Timestamps
~~~~~~~~~~~~~~~~~~~~~~~~~

*Tz-naive timestamp* is a term from Pandas. *Naive* in Python means
there is no timezone data present at all. Because this kind of timestamp
has no timezone defined, it can have no instant value. The `Java docs
for
``Class LocalDateTime`` <https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html>`__
call this kind of timestamp "a description of the date, as used for
birthdays, combined with the local time as seen on a wall clock."

An important issue is that a tz-naive timestamp looks just like a
timestamp with an implicit timezone, but they are not the same thing.
The essential difference is that the tz-native variety has no timezone
at all, not even implicitly.

To define an instant value for a tz-naive timestamp, localize it to a
specific timezone. For instance, given:

::

    1970-01-01 00:00:00

you can localize it to the ``UTC`` timezone:

::

    1970-01-01 00:00:00 UTC

or to the New York timezone:

::

    1970-01-01 00:00:00 America/New_York

Notice how this example differs from the example above of a timestamp
with an implicit timezone. Since the timestamp initially had no
timezone, neither implicit nor explicit, localizing it changes its
interpretation as a string, by appending a timezone substring.
Localizing it may also change its underlying numerical representation.
These last two timestamps both have explicit timezones, and represent
two different instants on an absolute timeline.

Interpretation of a tz-naive timestamp always returns the same value,
regardless of any timezone. For instance, given the tz-naive timestamp:

::

    1970-01-01 00:00:00

its ``HOUR`` value is always 0. Interpreting this kind of timestamp is
always literal; without localization to the locale of the user (or any
other timezone).

**Examples** of tz-naive timezones include:

-  Timestamps in core SQL (Feature ID F051-03).

-  Joda/Java8
   ```LocalDateTime`` <https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html>`__
   type.

-  Python/Pandas tz-naive timestamp.

-  Parquet floating timestamp
   (`proposed <https://issues.apache.org/jira/browse/PARQUET-905>`__).

Important Issues in Dealing with Timestamps
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. **Confusing different types**. Since tz-naive timestamps look the
   same as timestamps with implicit timezone, whenever you use the
   string representation of a timestamp that displays no timezone,
   clarify which kind it is.

#. **Internal representation**. Most systems use a numeric value to
   store tz-naive timestamp, representing the number of seconds (or
   milliseconds or microseconds in some systems) since the "Epoch,"
   meaning January 1, 1970.

For instance, a tz-naive timestamp of value 0 represents:

``1970-01-01 00:00:00``

But a timestamp of value 0 that has an explicit timezone of ``UTC``
represents the instant value:

``1970-01-01 00:00:00 UTC``

Timestamps can be negative in some system; a negative timestamp
represents the number of seconds (etc.) before the Unix Epoch.

#. **UTC-Normalized timestamps**. A timestamp is said to be
   *UTC-normalized* if its instant value is within the UTC timezone:
   that means UTC, rather than some other timezone, is the norm within
   which the instant value has meaning. Timestamps with either explicit
   or implicit timezones can be UTC-normalized, but not tz-naive
   timestamps. UTC-normalized is the usual state of affairs for most
   tz-aware timestamps.

--------------

Timestamps in Different Systems
-------------------------------

Arrow, Python, and Pandas
~~~~~~~~~~~~~~~~~~~~~~~~~

The Arrow timestamp consists of a numeric value (64-bit integer), a time
unit, and a timezone. Python and Pandas have essentially the same
timestamp concepts as Arrow, but with different names, and conversion is
straightforward. There are two varieties:

-  A *timestamp with timezone* is a UTC-normalized timestamp with
   explicit timezone. It is also called *aware* (Python) or *tz-aware*
   (Pandas).
-  A *timestamp without timezone* (``timezone`` = ``null``) is a
   tz-naive timestamp. It is also called *naive* (Python) or *tz-naive*
   (Pandas).

Spark
~~~~~

The Spark timestamp is a UTC-normalized fixed timestamp without
timezone. Spark timestamps differ from those in Arrow, Python, and
Pandas, so conversion is more complicated (see
`below <#timestamp-conversions-arrow--spark>`__).

--------------

Timestamp Conversions (Arrow ⇄ Spark)
-------------------------------------

Converting between timezones is a metadata-only operation and does not
change the underlying values.

Spark interprets timestamps by default with the *session local
timezone*, ``spark.sql.session.timeZone``. If that timezone is
undefined, Spark turns to the default system timezone. For simplicity's
sake below, assume the session local timezone is always defined.

Converting Arrow → Spark
~~~~~~~~~~~~~~~~~~~~~~~~

The following cases assume the Spark configuration
``spark.sql.execution.arrow.enabled`` is set to ``"true"``.

For Timestamps with Timezone
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set the timezone to session-local:

::

    >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
    >>> pdf1
                           time
    0 1970-01-01 00:00:00+00:00
    >>> spark.createDataFrame(pdf1).show()
    +-------------------+
    |               time|
    +-------------------+
    |1970-01-01 00:00:00|
    +-------------------+

Set the session timezone to New York:

::

    >>> spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    >>> pdf1
                           time
    0 1970-01-01 00:00:00+00:00

    >>> spark.createDataFrame(pdf1).show()
    +-------------------+
    |               time|
    +-------------------+
    |1969-12-31 19:00:00|
    +-------------------+

For Timestamps without Timezone
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Because Spark doesn't support tz-naive timestamps, you have localize
them when you pass them to Spark. Spark uses session local timezone for
this purpose:

::

    >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
    >>> pdf
            time
    0 1970-01-01

    >>> spark.createDataFrame(pdf).show()
    +-------------------+
    |               time|
    +-------------------+
    |1970-01-01 00:00:00|
    +-------------------+

Set the session timezone to New York:

::

    >>> spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    >>> pdf
            time
    0 1970-01-01

    >>> spark.createDataFrame(pdf).show()
    +-------------------+
    |               time|
    +-------------------+
    |1970-01-01 00:00:00|
    +-------------------+

Because the Pandas ``Timestamp`` object is tz-naive, Spark will localize
the timestamp with a session local timezone. As a result, in Spark this
timestamp:

::

    1970-01-01 00:00:00

can mean two different things, depending on the value of
``spark.sql.session.timeZone``. It can mean:

::

    1970-01-01 00:00:00 UTC

or it can mean:

::

    1970-01-01 00:00:00 America/New_York

Converting Spark → Arrow
~~~~~~~~~~~~~~~~~~~~~~~~

Set the session timezone to UTC:

::

    >>> spark.conf.set("spark.sql.session.timeZone", "UTC")
    >>> df.show()
    +-------------------+
    |               time|
    +-------------------+
    |1970-01-01 00:00:00|
    +-------------------+

    >>> df.toPandas()
            time
    0 1970-01-01

Set the session timezone to New York:

::

    >>> spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    >>> df.show()
    +-------------------+
    |               time|
    +-------------------+
    |1969-12-31 19:00:00|
    +-------------------+

    >>> df.toPandas()
                     time
    0 1969-12-31 19:00:00

Here we are converting the Spark timestamp to an Arrow tz-naive
timestamp in the session local timezone. The two timestamps in the
example represent the same instant:

::

    1970-01-01 00:00:00 UTC

In the UTC case, the converted Pandas timestamp ``1970-01-01`` is the
local time of the timestamp in the UTC timezone. But in the New York
case, the converted Pandas timestamp ``1969-12-31 19:00:00`` is the
local time of the timestamp in the ``America/New_York`` timezone.
