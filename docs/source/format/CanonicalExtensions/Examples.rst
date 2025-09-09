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

.. _format_canonical_extension_examples:

****************************
Canonical Extension Examples
****************************

=========================
Parquet Variant Extension
=========================


Unshredded
''''''''''

The simplest case, an unshredded variant always consists of **exactly** two fields: ``metadata`` and ``value``. Any of
the following storage types are valid (not an exhaustive list):

* ``struct<metadata: binary non-nullable, value: binary nullable>``
* ``struct<value: binary nullable, metadata: binary non-nullable>``
* ``struct<metadata: dictionary<int8, binary> non-nullable, value: binary_view nullable>``

Simple Shredding
''''''''''''''''

Suppose we have a Variant field named *measurement* and we want to shred the ``int64`` values into a separate column for efficiency.
In Parquet, this could be represented as::

  required group measurement (VARIANT) {
    required binary metadata;
    optional binary value;
    optional int64 typed_value;
  }

Thus the corresponding storage type for the ``arrow.parquet.variant`` Arrow extension type would be::

  struct<
    metadata: binary non-nullable,
    value: binary nullable,
    typed_value: int64 nullable
  >

If we suppose a series of measurements consisting of::

  34, null, "n/a", 100

The data should be stored/represented in Arrow as::

  * Length: 4, Null count: 1
  * Validity Bitmap buffer:

    | Byte 0 (validity bitmap) | Bytes 1-63    |
    |--------------------------|---------------|
    | 00001011                 | 0 (padding)   |

  * Children arrays:
    * field-0 array (`VarBinary`)
      * Length: 4, Null count: 0
      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 2, 4, 6, 8    | unspecified (padding)    |

      * Value buffer: (01 00 -> indicates version 1 empty metadata)

        | Bytes 0-7               | Bytes 8-63               |
        |-------------------------|--------------------------|
        | 01 00 01 00 01 00 01 00 | unspecified (padding)    |

    * field-1 array (`VarBinary`)
      * Length: 4, Null count: 2
      * Validity Bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63    |
        |--------------------------|---------------|
        | 00000110                 | 0 (padding)   |

      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 0, 1, 5, 5    | unspecified (padding)    |

      * Value buffer: (`00` -> null,
        `0x13 0x6E 0x2F 0x61` -> variant encoding literal string "n/a")

        | Bytes 0-4              | Bytes 5-63               |
        |------------------------|--------------------------|
        | 00 0x13 0x6E 0x2F 0x61 | unspecified (padding)    |

    * field-2 array (int64 array)
      * Length: 4, Null count: 2
      * Validity Bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63    |
        |--------------------------|---------------|
        | 00001001                 | 0 (padding)   |

      * Value buffer:

        | Bytes 0-31          | Bytes 32-63              |
        |---------------------|--------------------------|
        | 34, 00, 00, 100     | unspecified (padding)    |

.. note::

   Notice that there is a variant ``literal null`` in the ``value`` array, this is due to the
   `shredding specification <https://github.com/apache/parquet-format/blob/master/VariantShredding.md#value-shredding>`__
   so that a consumer can tell the difference between a *missing* field and a *null* field. A null
   element must be encoded as a Variant null: *basic type* ``0`` (primitive) and *physical type* ``0`` (null).

Shredding an Array
''''''''''''''''''

For our next example, we will represent a shredded array of strings. Let's consider a column that looks like: ::

  ["comedy", "drama"], ["horror", null], ["comedy", "drama", "romance"], null

Representing this shredded variant in Parquet could look like::

  optional group tags (VARIANT) {
    required binary metadata;
    optional binary value;
    optional group typed_value (LIST) { # optional to allow null lists
      repeated group list {
        required group element {        # shredded element
          optional binary value;
          optional binary typed_value (STRING);
        }
      }
    }
  }

The array structure for Variant encoding does not allow missing elements, so all elements of the array must
be *non-nullable*. As such, either **typed_value** or **value** (*but not both!*) must be *non-null*.

The storage type to represent this in Arrow as a Variant extension type would be::

  struct<
    metadata: binary non-nullable,
    value: binary nullable,
    typed_value: list<element: struct<
      value: binary nullable,
      typed_value: string nullable
    > non-nullable> nullable
  >

.. note::

   As usual, **Binary** could also be **LargeBinary** or **BinaryView**, **String** could also be **LargeString** or **StringView**,
   and **List** could also be **LargeList** or **ListView**.

The data would then be stored in Arrow as follows::

  * Length: 4, Null count: 1
  * Validity Bitmap buffer:

    | Byte 0 (validity bitmap) | Bytes 1-63    |
    |--------------------------|---------------|
    | 00000111                 | 0 (padding)   |

  * Children arrays:
    * field-0 array (`VarBinary` metadata)
      * Length: 4, Null count: 0
      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 2, 4, 6, 8    | unspecified (padding)    |

      * Value buffer: (01 00 -> indicates version 1 empty metadata)

        | Bytes 0-7               | Bytes 8-63               |
        |-------------------------|--------------------------|
        | 01 00 01 00 01 00 01 00 | unspecified (padding)    |

    * field-1 array (`VarBinary` value)
      * Length: 4, Null count: 1
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63    |
        |--------------------------|---------------|
        | 00001000                 | 0 (padding)   |

      * Offsets buffer:

        | Bytes 0-19       | Bytes 20-63              |
        |------------------|--------------------------|
        | 0, 0, 0, 0, 1    | unspecified (padding)    |

      * Value buffer: (00 -> variant null)

        | Bytes 0            | Bytes 1-63               |
        |--------------------|--------------------------|
        | 00                 | unspecified (padding)    |

    * field-2 array (`List<Struct<VarBinary, String>>` typed_value)
      * Length: 4, Null count: 1
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap) | Bytes 1-63  |
        |--------------------------|-------------|
        | 00000111                 | 0 (padding) |

      * Offsets buffer (int32)

        | Bytes 0-19        | Bytes 20-63           |
        |-------------------|-----------------------|
        | 0, 2, 4, 7, 7     | unspecified (padding) |

      * Values array (`Struct<VarBinary, String>` element):
        * Length: 7, Null count: 0
        * Validity bitmap buffer: Not required

        * Children arrays:
          * field-0 array (`VarBinary` value)
            * Length: 7, Null count: 6
            * Validity bitmap buffer:

              | Byte 0 (validity bitmap) | Bytes 1-63  |
              |--------------------------|-------------|
              | 00001000                 | 0 (padding) |

            * Offsets buffer (int32):

              | Bytes 0-31                | Bytes 32-63              |
              |---------------------------|--------------------------|
              | 0, 0, 0, 0, 1, 1, 1, 1    | unspecified (padding)    |

            * Values buffer (`00` -> variant null):

              | Bytes 0            | Bytes 1-63               |
              |--------------------|--------------------------|
              | 00                 | unspecified (padding)    |

          * field-1 array (`String` typed_value)
            * Length: 7, Null count: 1
            * Validity bitmap buffer:

              | Byte 0 (validity bitmap) | Bytes 1-63  |
              |--------------------------|-------------|
              | 01110111                 | 0 (padding) |

            * Offsets buffer (int32):

              | Bytes 0-31                      | Bytes 32-63              |
              |---------------------------------|--------------------------|
              | 0, 6, 11, 17, 17, 23, 28, 35    | unspecified (padding)    |

            * Values buffer:

              | Bytes 0-35                           | Bytes 36-63              |
              |--------------------------------------|--------------------------|
              | comedydramahorrorcomedydramaromance  | unspecified (padding)    |

Shredding an Object
'''''''''''''''''''

Let's consider a JSON column of "events" which contain a field named ``event_type`` (a string)
and a field named ``event_ts`` (a timestamp) that we wish to shred into separate columns, In Parquet,
it could look something like this::

  optional group event (VARIANT) {
    required binary metadata;
    optional binary value;        # variant, remaining fields/values
    optional group typed_value {  # shredded fields for variant object
      required group event_type { # event_type shredded field
        optional binary value;
        optional binary typed_value (STRING);
      }
      required group event_ts {   # event_ts shredded field
        optional binary value;
        optional int64 typed_value (TIMESTAMP(true, MICROS))
      }
    }
  }

We can then translate this into the expected extension storage type::

  struct<
    metadata: binary non-nullable,
    value: binary nullable,
    typed_value: struct<
      event_type: struct<
        value: binary nullable,
        typed_value: string nullable
      > non-nullable,
      event_ts: struct<
        value: binary nullable,
        typed_value: timestamp(us, UTC) nullable
      > non-nullable
    > nullable
  >

If a field *does not exist* in the variant object value, then both the **value** and **typed_value** columns for that row
will be null. If a field is *present*, but the value is null, then **value** must contain a Variant null.

It is *invalid* for both **value** and **typed_value** to be non-null for a given index. A reader can choose not to error
in this scenario, but if so it **must** use the value in the **typed_value** column for that index.

Let's consider the following series of objects::

  {"event_type": "noop", "event_ts": 1729794114937}

  {"event_type": "login", "event_ts": 1729794146402, "email": "user@example.com"}

  {"error_msg": "malformed..."}

  "malformed: not an object"

  {"event_ts": 1729794240241, "click": "_button"}

  {"event_ts": null, "event_ts": 1729794954163}

  {"event_type": "noop", "event_ts": "2024-10-24"}

  {}

  null

  *Entirely missing*

To represent those values as a column of Variant values using the Variant extension type we get the following::

  * Length: 10, Null count: 1
  * Validity bitmap buffer:

    | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
    |--------------------------|-----------|-----------------------|
    | 11111111                 | 00000001  | 0 (padding)           |

  * Children arrays
    * field-0 array (`VarBinary` Metadata)
      * Length: 10, Null count: 0
      * Offsets buffer:

        | Bytes 0-43 (int32)                       | Bytes 44-63             |
        |------------------------------------------|-------------------------|
        | 0, 2, 11, 24, 26, 35, 37, 39, 41, 43, 45 | unspecified (padding)   |

      * Value buffer: (01 00 -> version 1 empty metadata,
                       01 01 00 XX ... -> Version 1, metadata with 1 elem, offset 0, offset XX == len(string), ... is dict string bytes)

        | Bytes 0-1 | Bytes 2-10        | Bytes 11-23           | Bytes 24-25 | Bytes 26-34       |
        |-------------------------------|-----------------------|-------------|-------------------|
        | 01 00     | 01 01 00 05 email | 01 01 00 09 error_msg | 01 00       | 01 01 00 05 click |

        | Bytes 35-36 | Bytes 37-38 | Bytes 39-40 | Bytes 41-42 | Bytes 43-44 | Bytes 45-63           |
        |-------------|-------------|-------------|-------------|-------------|-----------------------|
        | 01 00       | 01 00       | 01 00       | 01 00       | 01 00       | unspecified (padding) |

    * field-1 array (`VarBinary` Value)
      * Length: 10, Null count: 5
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap)  | Byte 1    | Bytes 2-63            |
        |---------------------------|-----------|-----------------------|
        | 00011110                  | 00000001  | 0 (padding)           |

      * Offsets buffer (filled in based on lengths of encoded variants):

        | ... |

      * Value buffer:

        | VariantEncode({"email": "user@email.com"}) | VariantEncode({"error_msg": "malformed..."}) |
        | VariantEncode("malformed: not an object")  | VariantEncode({"click": "_button"})          | 00 (null) |

    * field-2 array (`Struct<...>` typed_value)
      * Length: 10, Null count: 3
      * Validity bitmap buffer:

        | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
        |--------------------------|-----------|-----------------------|
        | 11110111                 | 00000000  | 0 (padding)           |

      * Children arrays:
        * field-0 array (`Struct<VarBinary, String>` event_type)
          * Length: 10, Null count: 0
          * Validity bitmap buffer: not required

          * Children arrays
            * field-0 array (`VarBinary` value)
              * Length: 10, Null count: 9
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 01000000                 | 00000000  | 0 (padding)           |

              * Offsets buffer (int32)

                | Bytes 0-43 (int32)              | Bytes 44-63             |
                |---------------------------------|-------------------------|
                | 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1 | unspecified (padding)   |

              * Value buffer:

                | Byte 0 | Bytes 1-63             |
                |--------|------------------------|
                | 00     | unspecified (padding)  |

            * field-1 array (`String` typed_value)
              * Length: 10, Null count: 7
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 01000011                 | 00000000  | 0 (padding)           |

              * Offsets buffer (int32)

                | Byte 0-43                           | Bytes 44-63            |
                |-------------------------------------|------------------------|
                | 0, 4, 9, 9, 9, 9, 9, 13, 13, 13, 13 | unspecified (padding)  |

              * Value buffer:

                | Bytes 0-3 | Bytes 4-8 | Bytes 9-12 | Bytes 13-63            |
                |-----------|-----------|------------|------------------------|
                | noop      | login     | noop       | unspecified (padding)  |


        * field-1 array (`Struct<VarBinary, Timestamp>` event_ts)
          * Length: 10, Null count: 0
          * Validity bitmap buffer: not required

          * Children arrays
            * field-0 array (`VarBinary` value)
              * Length: 10, Null count: 9
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 01000000                 | 00000000  | 0 (padding)           |

              * Offsets buffer (int32)

                | Bytes 0-43 (int32)              | Bytes 44-63             |
                |---------------------------------|-------------------------|
                | ...                             | unspecified (padding)   |

              * Value buffer:

                | VariantEncode("2024-10-24")     |

            * field-1 array (`Timestamp(us, UTC)` typed_value)
              * Length: 10, Null count: 6
              * Validity bitmap buffer:

                | Byte 0 (validity bitmap) | Byte 1    | Bytes 2-63            |
                |--------------------------|-----------|-----------------------|
                | 00110011                 | 00000000  | 0 (padding)           |

              * Value buffer:

                | Bytes 0-7     | Bytes 8-15    | Bytes 16-31  | Bytes 32-39   | Bytes 40-47   | Bytes 48-63            |
                |---------------|---------------|--------------|---------------|---------------|------------------------|
                | 1729794114937 | 1729794146402 | unspecified  | 1729794240241 | 1729794954163 | unspecified (padding)  |


Putting it all together
'''''''''''''''''''''''

As mentioned, the **typed_value** field associated with a Variant **value** can be of any shredded type. As a result,
as long as we follow the original rules we can have an arbitrary number of nested levels based on how you want to
shred the object. For example, we might have a few more fields alongside **event_type** to shred out. Possibly an object
that looks like this::

  {
    "event_type": "login",
    "event_ts": 1729794114937,
    "location”: {"longitude": 1.5, "latitude": 5.5},
    "tags": ["foo", "bar", "baz"]
  }

If we shred the extra fields out and represent it as Parquet it looks like::

  optional group event (VARIANT) {
    required binary metadata;
    optional binary value;        # variant, remaining fields/values
    optional group typed_value {  # shredded fields for variant object
      required group event_type { # event_type shredded field
        optional binary value;
        optional binary typed_value (STRING);
      }
      required group event_ts {   # event_ts shredded field
        optional binary value;
        optional int64 typed_value (TIMESTAMP(true, MICROS))
      }
      required group location {   # location shredded field
        optional binary value;
        optional group typed_value {
          required group longitude {
            optional binary value;
            optional float64 typed_value;
          }
          required group latitude {
            optional binary value;
            optional float64 typed_value;
          }
        }
      }
      required group tags {       # tags shredded field
        optional binary value;
        optional group typed_value (LIST) {
          repeated group list {
            required group element {
              optional binary value;
              optional binary typed_value (STRING);
            }
          }
        }
      }
    }
  }

Finally, following the rules we set forth on constructing the Variant Extension Type storage type, we end up with::

  struct<
    metadata: binary non-nullable,
    value: binary nullable,
    typed_value: struct<
      event_type: struct<value: binary nullable, typed_value: string nullable> non-nullable,
      event_ts: struct<value: binary nullable, typed_value: timestamp(us, UTC) nullable> non-nullable,
      location: struct<
        value: binary nullable,
        typed_value: struct<
          longitude: struct<value: binary nullable, typed_value: double nullable> non-nullable,
          latitude: struct<value: binary nullable, typed_value: double nullable> non-nullable
        > nullable> non-nullable,
      tags: struct<
          value: binary nullable,
          typed_value: list<struct<value: binary nullable, typed_value: string nullable> non-nullable> nullable
        > non-nullable
    > nullable
  >

