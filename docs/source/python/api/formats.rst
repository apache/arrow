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

Tabular File Formats
====================

.. _api.csv:

CSV Files
---------

.. currentmodule:: pyarrow.csv

.. autosummary::
   :toctree: ../generated/

   ReadOptions
   ParseOptions
   ConvertOptions
   read_csv

.. _api.feather:

Feather Files
-------------

.. currentmodule:: pyarrow.feather

.. autosummary::
   :toctree: ../generated/

   read_feather
   write_feather

.. _api.json:

JSON Files
----------

.. currentmodule:: pyarrow.json

.. autosummary::
   :toctree: ../generated/

   ReadOptions
   ParseOptions
   read_json

.. _api.parquet:

Parquet Files
-------------

.. currentmodule:: pyarrow.parquet

.. autosummary::
   :toctree: ../generated/

   ParquetDataset
   ParquetFile
   ParquetWriter
   read_table
   read_metadata
   read_pandas
   read_schema
   write_metadata
   write_table
   write_to_dataset
