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



.. _data-model:

Data model
==========


.. graphviz:: data_model.dot


benchmark_view
-------------

The details about a particular benchmark.

- "benchmark_name" is unique for a given "benchmark_language"
- Each entry is unique on
  ("benchmark_language", "benchmark_name", "benchmark_version")

=====================  ======  ========  =======  ===================
Column                 Type    Nullable  Default  Description
=====================  ======  ========  =======  ===================
benchmark_id           int4    not null  serial   primary key, unique
benchmark_name         citext  not null           unique
parameter_names        _text
benchmark_description  text    not null
benchmark_type         citext  not null           unique
units                  citext  not null           unique
lessisbetter           bool    not null
benchmark_version      citext  not null           unique
benchmark_language     citext  not null           unique
=====================  ======  ========  =======  ===================


back to `Data model <data-model>`_





.. _data-model:

Data model
==========


.. graphviz:: data_model.dot


environment_view
---------------

The build environment used for a reported benchmark run.
(Will be inferred from each "benchmark_run" if not expicitly added).

- Each entry is unique on
  ("benchmark_language", "language_implementation_version", "dependencies")
- "benchmark_language" is unique in the "benchmark_language" table
- "benchmark_language" plus "language_implementation_version" is unique in
  the "language_implementation_version" table
- "dependencies" is unique in the "dependencies" table

===============================  ======  ========  ===========  ===================
Column                           Type    Nullable  Default      Description
===============================  ======  ========  ===========  ===================
environment_id                   int4    not null  serial       primary key, unique
benchmark_language               citext  not null               unique
language_implementation_version  citext  not null  ''::citext   unique
dependencies                     jsonb   not null  '{}'::jsonb  unique
===============================  ======  ========  ===========  ===================


back to `Data model <data-model>`_



.. _data-model:

Data model
==========


.. graphviz:: data_model.dot


machine_view
-----------

The machine environment (CPU, GPU, OS) used for each benchmark run.

- "mac_address" is unique in the "machine" table
- "gpu_part_number" is unique in the "gpu" (graphics processing unit) table
  Empty string (''), not null, is used for machines that won't use the GPU
- "cpu_model_name" is unique in the "cpu" (central processing unit) table
- "os_name", "os_architecture_name", and "os_kernel_name"
  are unique in the "os" (operating system) table
- "machine_other_attributes" is a key-value store for any other relevant
  data, e.g. '{"hard_disk_type": "solid state"}'

========================  =======  ========  ==========  ===================
Column                    Type     Nullable  Default     Description
========================  =======  ========  ==========  ===================
machine_id                int4     not null  serial      primary key, unique
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
========================  =======  ========  ==========  ===================


back to `Data model <data-model>`_



.. _data-model:

Data model
==========


.. graphviz:: data_model.dot


benchmark_run_view
-----------------

Each benchmark run.

- Each entry is unique on the machine, environment, benchmark,
  and git commit timestamp.

===============================  ===========  ========  ===========  ===================
Column                           Type         Nullable  Default      Description
===============================  ===========  ========  ===========  ===================
benchmark_run_id                 int8         not null  serial       primary key, unique
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
===============================  ===========  ========  ===========  ===================


back to `Data model <data-model>`_



