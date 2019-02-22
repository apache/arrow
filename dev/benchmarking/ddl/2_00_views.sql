/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

-- NOTE:
-- The function for documentation depends on view columns
-- being named exactly the same as in the table view.

-- MACHINE_VIEW
CREATE OR REPLACE VIEW public.machine_view AS
  SELECT
    machine.machine_id
    , mac_address
    , machine_name
    , memory_bytes
    , cpu_actual_frequency_Hz
    , os_name
    , architecture_name
    , kernel_name
    , cpu_model_name
    , cpu_core_count
    , cpu_thread_count
    , cpu_frequency_max_Hz
    , cpu_frequency_min_Hz
    , cpu_L1d_cache_bytes
    , cpu_L1i_cache_bytes
    , cpu_L2_cache_bytes
    , cpu_L3_cache_bytes
    , gpu_information
    , gpu_part_number
    , gpu_product_name
    , machine_other_attributes
  FROM public.machine AS machine
    JOIN public.cpu AS cpu ON machine.cpu_id = cpu.cpu_id
    JOIN public.gpu AS gpu ON machine.gpu_id = gpu.gpu_id
    JOIN public.os AS os ON machine.os_id = os.os_id;
COMMENT ON VIEW public.machine_view IS
E'The machine environment (CPU, GPU, OS) used for each benchmark run.\n\n'
 '- "mac_address" is unique in the "machine" table\n'
 '- "gpu_part_number" is unique in the "gpu" (graphics processing unit) table\n'
 '  Empty string (''''), not null, is used for machines that won''t use the GPU\n'
 '- "cpu_model_name" is unique in the "cpu" (central processing unit) table\n'
 '- "os_name", "os_architecture_name", and "os_kernel_name"\n'
 '  are unique in the "os" (operating system) table\n'
 '- "machine_other_attributes" is a key-value store for any other relevant\n'
 '  data, e.g. ''{"hard_disk_type": "solid state"}''';


-- LANGUAGE_IMPLEMENTATION_VERSION_VIEW
CREATE OR REPLACE VIEW public.language_implementation_version_view AS
  SELECT
    lv.language_implementation_version_id
    , bl.benchmark_language
    , lv.language_implementation_version
  FROM public.language_implementation_version AS lv
    JOIN public.benchmark_language AS bl
      ON lv.benchmark_language_id = bl.benchmark_language_id;

-- ENVIRONMENT_VIEW
CREATE OR REPLACE VIEW public.environment_view AS
  SELECT
    env.environment_id
    , benchmark_language
    , language_implementation_version
    , dependencies
  FROM public.environment AS env
    JOIN public.benchmark_language AS language
      ON env.benchmark_language_id = language.benchmark_language_id
    JOIN public.language_implementation_version AS version
      ON env.language_implementation_version_id = version.language_implementation_version_id
    JOIN public.dependencies AS deps
      ON env.dependencies_id = deps.dependencies_id;
COMMENT ON VIEW public.environment_view IS
E'The build environment used for a reported benchmark run.\n'
 '(Will be inferred from each "benchmark_run" if not expicitly added).\n\n'
 '- Each entry is unique on\n'
 '  ("benchmark_language", "language_implementation_version", "dependencies")\n'
 '- "benchmark_language" is unique in the "benchmark_language" table\n'
 '- "benchmark_language" plus "language_implementation_version" is unique in\n'
 '  the "language_implementation_version" table\n'
 '- "dependencies" is unique in the "dependencies" table';

-- UNIT_VIEW
CREATE OR REPLACE VIEW public.unit_view AS
  SELECT
    unit.unit_id
    , units
    , benchmark_type
    , lessisbetter
  FROM public.unit AS unit
    JOIN public.benchmark_type AS bt
      ON unit.benchmark_type_id = bt.benchmark_type_id;

-- BENCHMARK_VIEW
CREATE OR REPLACE VIEW public.benchmark_view AS
  SELECT
    b.benchmark_id
    , benchmark_name
    , parameter_names
    , benchmark_description
    , benchmark_type
    , units
    , lessisbetter
    , benchmark_version
    , benchmark_language
  FROM public.benchmark AS b
    JOIN public.benchmark_language AS benchmark_language
      ON b.benchmark_language_id = benchmark_language.benchmark_language_id
    JOIN public.unit AS unit
      ON b.unit_id = unit.unit_id
    JOIN public.benchmark_type AS benchmark_type
      ON unit.benchmark_type_id = benchmark_type.benchmark_type_id;
COMMENT ON VIEW public.benchmark_view IS
E'The details about a particular benchmark.\n\n'
 '- "benchmark_name" is unique for a given "benchmark_language"\n'
 '- Each entry is unique on\n'
 '  ("benchmark_language", "benchmark_name", "benchmark_version")';

-- BENCHMARK_RUN_VIEW
CREATE OR REPLACE VIEW public.benchmark_run_view AS
  SELECT
    run.benchmark_run_id
    -- benchmark_view (name, version, language only)
    , benchmark_name
    , benchmark_version
    -- datum
    , parameter_values
    , value
    , git_commit_timestamp
    , git_hash
    , val_min
    , val_q1
    , val_q3
    , val_max
    , std_dev
    , n_obs
    , run_timestamp
    , run_metadata
    , run_notes
    -- machine_view (mac address only)
    , mac_address
    -- environment_view
    , env.benchmark_language
    , language_implementation_version
    , dependencies
  FROM public.benchmark_run AS run
    JOIN public.benchmark_view AS benchmark
      ON run.benchmark_id = benchmark.benchmark_id
    JOIN public.machine_view AS machine
      ON run.machine_id = machine.machine_id
    JOIN public.environment_view AS env
      ON run.environment_id = env.environment_id;
COMMENT ON VIEW public.benchmark_run_view IS
E'Each benchmark run.\n\n'
 '- Each entry is unique on the machine, environment, benchmark,\n'
 '  and git commit timestamp.';

-- FULL_BENCHMARK_RUN_VIEW
CREATE OR REPLACE VIEW public.full_benchmark_run_view AS
  SELECT
    run.benchmark_run_id
    -- benchmark_view
    , benchmark_name
    , parameter_names
    , benchmark_description
    , benchmark_type
    , units
    , lessisbetter
    , benchmark_version
    -- datum
    , parameter_values
    , value
    , git_commit_timestamp
    , git_hash
    , val_min
    , val_q1
    , val_q3
    , val_max
    , std_dev
    , n_obs
    , run_timestamp
    , run_metadata
    , run_notes
    -- machine_view
    , machine_name
    , mac_address
    , memory_bytes
    , cpu_actual_frequency_Hz
    , os_name
    , architecture_name
    , kernel_name
    , cpu_model_name
    , cpu_core_count
    , cpu_thread_count
    , cpu_frequency_max_Hz
    , cpu_frequency_min_Hz
    , cpu_L1d_cache_bytes
    , cpu_L1i_cache_bytes
    , cpu_L2_cache_bytes
    , cpu_L3_cache_bytes
    , gpu_information
    , gpu_part_number
    , gpu_product_name
    , machine_other_attributes
    -- environment_view
    , env.benchmark_language
    , env.language_implementation_version
    , dependencies
  FROM public.benchmark_run AS run
    JOIN public.benchmark_view AS benchmark
      ON run.benchmark_id = benchmark.benchmark_id
    JOIN public.machine_view AS machine
      ON run.machine_id = machine.machine_id
    JOIN public.environment_view AS env
      ON run.environment_id = env.environment_id;
