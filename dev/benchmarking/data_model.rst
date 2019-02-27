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


.. WARNING
..   This is an auto-generated file. Please do not edit.

..   To reproduce, please run :code:`./make_data_model_rst.sh`.
..   (This requires you have the
..   `psql client <https://www.postgresql.org/download/>`_
..   and have started the docker containers using
..   :code:`docker-compose up`).


.. _benchmark-data-model:

Benchmark data model
====================


.. graphviz:: data_model.dot


.. _benchmark-ingestion:

Benchmark ingestion helper functions
====================================

ingest_benchmark_run_view
-------------------------

:code:`ingest_benchmark_run_view(from_jsonb jsonb)`

The argument is a JSON object. NOTE: key names must be entirely
lowercase, or the insert will fail. Extra key-value pairs are ignored.
Example::

  [
    {
      "benchmark_name": "Benchmark 2",
      "benchmark_version": "version 0",
      "parameter_values": {"arg0": 100, "arg1": 5},
      "value": 2.5,
      "git_commit_timestamp": "2019-02-08 22:35:53 +0100",
      "git_hash": "324d3cf198444a",
      "val_min": 1,
      "val_q1": 2,
      "val_q3": 3,
      "val_max": 4,
      "std_dev": 1.41,
      "n_obs": 8,
      "run_timestamp": "2019-02-14 03:00:05 -0600",
      "mac_address": "08:00:2b:01:02:03",
      "benchmark_language": "Python",
      "language_implementation_version": "CPython 2.7",
      "dependencies": {"six": "", "numpy": "1.14", "other_lib": "1.0"}
    },
    {
      "benchmark_name": "Benchmark 2",
      "benchmark_version": "version 0",
      "parameter_values": {"arg0": 1000, "arg1": 5},
      "value": 5,
      "git_commit_timestamp": "2019-02-08 22:35:53 +0100",
      "git_hash": "324d3cf198444a",
      "std_dev": 3.14,
      "n_obs": 8,
      "run_timestamp": "2019-02-14 03:00:10 -0600",
      "mac_address": "08:00:2b:01:02:03",
      "benchmark_language": "Python",
      "language_implementation_version": "CPython 2.7",
      "dependencies": {"six": "", "numpy": "1.14", "other_lib": "1.0"}
    }
  ]
To identify which columns in "benchmark_run_view" are required,
please see the view documentation in :ref:`benchmark-data-model`.



back to `Benchmark data model <benchmark-data-model>`_


ingest_benchmark_view
---------------------

:code:`ingest_benchmark_view(from_jsonb jsonb)`

The argument is a JSON object. NOTE: key names must be entirely
lowercase, or the insert will fail. Extra key-value pairs are ignored.
Example::

  [
    {
      "benchmark_name": "Benchmark 1",
      "parameter_names": ["arg0", "arg1", "arg2"],
      "benchmark_description": "First benchmark",
      "benchmark_type": "Time",
      "units": "miliseconds",
      "lessisbetter": true,
      "benchmark_version": "second version",
      "benchmark_language": "Python"
    },
    {
      "benchmark_name": "Benchmark 2",
      "parameter_names": ["arg0", "arg1"],
      "benchmark_description": "Description 2.",
      "benchmark_type": "Time",
      "units": "nanoseconds",
      "lessisbetter": true,
      "benchmark_version": "second version",
      "benchmark_language": "Python"
    }
  ]

To identify which columns in "benchmark_view" are required,
please see the view documentation in :ref:`benchmark-data-model`.



back to `Benchmark data model <benchmark-data-model>`_


ingest_benchmark_runs_with_context
----------------------------------

:code:`ingest_benchmark_runs_with_context(from_jsonb jsonb)`

The argument is a JSON object. NOTE: key names must be entirely
lowercase, or the insert will fail. Extra key-value pairs are ignored.
The object contains three key-value pairs::

    {"context": {
        "mac_address": "08:00:2b:01:02:03",
        "benchmark_language": "Python",
        "language_implementation_version": "CPython 3.6",
        "dependencies": {"six": "", "numpy": "1.14", "other_lib": "1.0"},
        "git_commit_timestamp": "2019-02-14 22:42:22 +0100",
        "git_hash": "123456789abcde",
        "run_timestamp": "2019-02-14 03:00:40 -0600",
        "extra stuff": "does not hurt anything and will not be added."
      },
     "benchmark_version": {
        "Benchmark Name 1": "Any string can be a version.",
        "Benchmark Name 2": "A git hash can be a version.",
        "An Unused Benchmark Name": "Will be ignored."
      },
     "benchmarks": [
        {
          "benchmark_name": "Benchmark Name 1",
          "parameter_values": {"argument1": 1, "argument2": "value2"},
          "value": 42,
          "val_min": 41.2,
          "val_q1":  41.5,
          "val_q3":  42.5,
          "val_max": 42.8,
          "std_dev": 0.5,
          "n_obs": 100,
          "run_metadata": {"any": "key-value pairs"},
          "run_notes": "Any relevant notes."
        },
        {
          "benchmark_name": "Benchmark Name 2",
          "parameter_values": {"not nullable": "Use {} if no params."},
          "value": 8,
          "std_dev": 1,
          "n_obs": 2,
        }
      ]
    }

- The entry for "context" contains the machine, environment, and timestamp
  information common to all of the runs
- The entry for "benchmark_version" maps benchmark
  names to their version strings. (Which can be a git hash,
  the entire code string, a number, or any other string of your choice.)
- The entry for "benchmarks" is a list of benchmark run data
  for the given context and benchmark versions. The first example
  benchmark run entry contains all possible values, even
  nullable ones, and the second entry omits all nullable values.




back to `Benchmark data model <benchmark-data-model>`_


ingest_machine_view
-------------------

:code:`ingest_machine_view(from_jsonb jsonb)`

The argument is a JSON object. NOTE: key names must be entirely
lowercase, or the insert will fail. Extra key-value pairs are ignored.
Example::

  {
    "mac_address": "0a:00:2d:01:02:03",
    "machine_name": "Yet-Another-Machine-Name",
    "memory_bytes": 8589934592,
    "cpu_actual_frequency_hz": 2300000000,
    "os_name": "OSX",
    "architecture_name": "x86_64",
    "kernel_name": "18.2.0",
    "cpu_model_name": "Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz",
    "cpu_core_count": 2,
    "cpu_thread_count": 4,
    "cpu_frequency_max_hz": 2300000000,
    "cpu_frequency_min_hz": 2300000000,
    "cpu_l1d_cache_bytes": 32768,
    "cpu_l1i_cache_bytes": 32768,
    "cpu_l2_cache_bytes": 262144,
    "cpu_l3_cache_bytes": 4194304,
    "machine_other_attributes": {"just": "an example"},
    "gpu_information": "",
    "gpu_part_number": "",
    "gpu_product_name": ""
  }

To identify which columns in "machine_view" are required,
please see the view documentation in :ref:`benchmark-data-model`.



back to `Benchmark data model <benchmark-data-model>`_



.. _benchmark-views:

Benchmark views
===============


benchmark_run_view
------------------

Each benchmark run.

- Each entry is unique on the machine, environment, benchmark,
  and git commit timestamp.

===============================  ===========  ========  ===========  ===========
Column                           Type         Nullable  Default      Description
===============================  ===========  ========  ===========  ===========
benchmark_run_id                 int8         not null  serial       primary key
benchmark_name                   citext       not null               unique
benchmark_version                citext       not null               unique
parameter_values                 jsonb        not null  '{}'::jsonb  unique
value                            numeric      not null
git_commit_timestamp             timestamptz  not null               unique
git_hash                         text         not null
val_min                          numeric
val_q1                           numeric
val_q3                           numeric
val_max                          numeric
std_dev                          numeric      not null
n_obs                            int4         not null
run_timestamp                    timestamptz  not null               unique
run_metadata                     jsonb
run_notes                        text
mac_address                      macaddr      not null               unique
benchmark_language               citext       not null               unique
language_implementation_version  citext       not null  ''::citext   unique
dependencies                     jsonb        not null  '{}'::jsonb  unique
===============================  ===========  ========  ===========  ===========

back to `Benchmark data model <benchmark-data-model>`_

benchmark_view
--------------

The details about a particular benchmark.

- "benchmark_name" is unique for a given "benchmark_language"
- Each entry is unique on
  ("benchmark_language", "benchmark_name", "benchmark_version")

=====================  ======  ========  =======  ===========
Column                 Type    Nullable  Default  Description
=====================  ======  ========  =======  ===========
benchmark_id           int4    not null  serial   primary key
benchmark_name         citext  not null           unique
parameter_names        _text
benchmark_description  text    not null
benchmark_type         citext  not null           unique
units                  citext  not null           unique
lessisbetter           bool    not null
benchmark_version      citext  not null           unique
benchmark_language     citext  not null           unique
=====================  ======  ========  =======  ===========

back to `Benchmark data model <benchmark-data-model>`_

environment_view
----------------

The build environment used for a reported benchmark run.
(Will be inferred from each "benchmark_run" if not expicitly added).

- Each entry is unique on
  ("benchmark_language", "language_implementation_version", "dependencies")
- "benchmark_language" is unique in the "benchmark_language" table
- "benchmark_language" plus "language_implementation_version" is unique in
  the "language_implementation_version" table
- "dependencies" is unique in the "dependencies" table

===============================  ======  ========  ===========  ===========
Column                           Type    Nullable  Default      Description
===============================  ======  ========  ===========  ===========
environment_id                   int4    not null  serial       primary key
benchmark_language               citext  not null               unique
language_implementation_version  citext  not null  ''::citext   unique
dependencies                     jsonb   not null  '{}'::jsonb  unique
===============================  ======  ========  ===========  ===========

back to `Benchmark data model <benchmark-data-model>`_

machine_view
------------

The machine environment (CPU, GPU, OS) used for each benchmark run.

- "mac_address" is unique in the "machine" table
- "gpu_part_number" is unique in the "gpu" (graphics processing unit) table
  Empty string (''), not null, is used for machines that won't use the GPU
- "cpu_model_name" is unique in the "cpu" (central processing unit) table
- "os_name", "os_architecture_name", and "os_kernel_name"
  are unique in the "os" (operating system) table
- "machine_other_attributes" is a key-value store for any other relevant
  data, e.g. '{"hard_disk_type": "solid state"}'

========================  =======  ========  ==========  ===========
Column                    Type     Nullable  Default     Description
========================  =======  ========  ==========  ===========
machine_id                int4     not null  serial      primary key
mac_address               macaddr  not null              unique
machine_name              citext   not null
memory_bytes              int8     not null
cpu_actual_frequency_hz   int8     not null
os_name                   citext   not null              unique
architecture_name         citext   not null              unique
kernel_name               citext   not null  ''::citext  unique
cpu_model_name            citext   not null              unique
cpu_core_count            int4     not null
cpu_thread_count          int4     not null
cpu_frequency_max_hz      int8     not null
cpu_frequency_min_hz      int8     not null
cpu_l1d_cache_bytes       int4     not null
cpu_l1i_cache_bytes       int4     not null
cpu_l2_cache_bytes        int4     not null
cpu_l3_cache_bytes        int4     not null
gpu_information           citext   not null  ''::citext  unique
gpu_part_number           citext   not null  ''::citext
gpu_product_name          citext   not null  ''::citext
machine_other_attributes  jsonb
========================  =======  ========  ==========  ===========

back to `Benchmark data model <benchmark-data-model>`_


