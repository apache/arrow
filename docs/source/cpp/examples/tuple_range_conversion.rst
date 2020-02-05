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

.. default-domain:: cpp
.. highlight:: cpp

Conversion of range of ``std::tuple``-like to ``Table`` instances
=================================================================

While the above example shows a quite manual approach of a row to columnar
conversion, Arrow also provides some template logic to convert ranges of
``std::tuple<..>``-like objects to tables.

In the most simple case, you only need to provide the input data and the
type conversion is then inferred at compile time.

.. code::

   std::vector<std::tuple<double, std::string>> rows = ..
   std::shared_ptr<Table> table;

   if (!arrow::stl::TableFromTupleRange(
         arrow::default_memory_pool(),
         rows, names, &table).ok()
   ) {
     // Error handling code should go here.
   }

In reverse, you can use ``TupleRangeFromTable`` to fill an already
pre-allocated range with the data from a ``Table`` instance.

.. code::

    // An important aspect here is that the table columns need to be in the
    // same order as the columns will later appear in the tuple. As the tuple
    // is unnamed, matching is done on positions.
    std::shared_ptr<Table> table = ..

    // The range needs to be pre-allocated to the respective amount of rows.
    // This allows us to pass in an arbitrary range object, not only
    // `std::vector`.
    std::vector<std::tuple<double, std::string>> rows(2);
    if (!arrow::stl::TupleRangeFromTable(*table, &rows).ok()) {
      // Error handling code should go here.
    }

Arrow itself already supports some C(++) data types for this conversion. If you
want to support additional data types, you need to implement a specialization
of ``arrow::stl::ConversionTraits<T>`` and the more general
``arrow::CTypeTraits<T>``.


.. code::

    namespace arrow {

    template<>
    struct CTypeTraits<boost::posix_time::ptime> {
      using ArrowType = ::arrow::TimestampType;

      static std::shared_ptr<::arrow::DataType> type_singleton() {
        return ::arrow::timestamp(::arrow::TimeUnit::MICRO);
      }
    };

    }

    namespace arrow { namespace stl {

    template <>
    struct ConversionTraits<boost::posix_time::ptime> : public CTypeTraits<boost::posix_time::ptime> {
      constexpr static bool nullable = false;

      // This is the specialization to load a scalar value into an Arrow builder.
      static Status AppendRow(
            typename TypeTraits<TimestampType>::BuilderType& builder,
            boost::posix_time::ptime cell) {
        boost::posix_time::ptime const epoch({1970, 1, 1}, {0, 0, 0, 0});
        return builder.Append((cell - epoch).total_microseconds());
      }

      // Specify how we can fill the tuple from the values stored in the Arrow
      // array.
      static boost::posix_time::ptime GetEntry(
            const TimestampArray& array, size_t j) {
        return psapp::arrow::internal::timestamp_epoch
            + boost::posix_time::time_duration(0, 0, 0, array.Value(j));
      }
    };

    }}

