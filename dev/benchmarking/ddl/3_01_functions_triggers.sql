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


-------------------------- TRIGGER FUNCTIONS --------------------------
-- Views that do not select from a single table or view are not
-- automatically updatable. These trigger functions are intended
-- to be run instead of INSERT into the complicated views.


-- LANGUAGE_IMPLEMENTATION_VERSION_VIEW_INSERT_ROW
CREATE OR REPLACE FUNCTION public.language_implementation_version_view_insert_row()
RETURNS trigger AS
$$
  DECLARE
    language_id integer;
    result integer;
  BEGIN
    IF NEW.benchmark_language IS NULL THEN
      RAISE EXCEPTION 'Column "benchmark_language" cannot be NULL.';
    END IF;
    IF NEW.language_implementation_version IS NULL THEN
      RAISE EXCEPTION
        'Column "language_implementation_version" cannot be NULL (use '''' instead).';
    END IF;

    SELECT public.get_benchmark_language_id(NEW.benchmark_language)
      INTO language_id;

    SELECT language_implementation_version_id INTO result FROM public.language_implementation_version AS lv
    WHERE lv.benchmark_language_id = language_id
      AND lv.language_implementation_version = NEW.language_implementation_version;

    IF result IS NOT NULL THEN
      -- row already exists
      RETURN NULL;
    ELSE
      INSERT INTO
        public.language_implementation_version(
          benchmark_language_id
          , language_implementation_version
        )
      VALUES (language_id, NEW.language_implementation_version)
      RETURNING language_implementation_version_id INTO NEW.language_implementation_version_id;
    END IF;

    RETURN NEW;
  END
$$
LANGUAGE plpgsql;

-- ENVIRONMENT_VIEW_INSERT_ROW
CREATE OR REPLACE FUNCTION public.environment_view_insert_row()
RETURNS trigger AS
$$
  DECLARE
    found_language_id integer;
    found_version_id integer;
    found_dependencies_id integer;
    result integer;
  BEGIN
    IF NEW.benchmark_language IS NULL
    THEN
      RAISE EXCEPTION 'Column "benchmark_language" cannot be NULL.';
    END IF;
    IF NEW.language_implementation_version IS NULL THEN
      RAISE EXCEPTION
        'Column "language_implementation_version" cannot be NULL (use '''' instead).';
    END IF;

    SELECT public.get_benchmark_language_id(NEW.benchmark_language)
      INTO found_language_id;

    SELECT public.get_language_implementation_version_id(
      found_language_id
      , NEW.language_implementation_version
    )
    INTO found_version_id;

    SELECT public.get_dependencies_id(NEW.dependencies)
    INTO found_dependencies_id;

    SELECT environment_id INTO result FROM public.environment AS e
    WHERE e.benchmark_language_id = found_language_id
      AND e.language_implementation_version_id = found_version_id
      AND e.dependencies_id = found_dependencies_id;

    IF result IS NOT NULL THEN
      -- row already exists
      RETURN NULL;
    ELSE
      INSERT INTO
        public.environment(
          benchmark_language_id
          , language_implementation_version_id
          , dependencies_id
        )
      VALUES (found_language_id, found_version_id, found_dependencies_id)
      RETURNING environment_id INTO NEW.environment_id;
    END IF;

    RETURN NEW;
  END
$$
LANGUAGE plpgsql;

-- MACHINE_VIEW_INSERT_ROW
CREATE OR REPLACE FUNCTION public.machine_view_insert_row()
RETURNS trigger AS
$$
  DECLARE
    found_cpu_id integer;
    found_gpu_id integer;
    found_os_id integer;
    result integer;
  BEGIN
    IF (
      NEW.machine_name IS NULL
      OR NEW.memory_bytes IS NULL
      OR NEW.cpu_model_name IS NULL
      OR NEW.cpu_core_count IS NULL
      OR NEW.cpu_thread_count IS NULL
      OR NEW.cpu_frequency_max_Hz IS NULL
      OR NEW.cpu_frequency_min_Hz IS NULL
      OR NEW.cpu_L1d_cache_bytes IS NULL
      OR NEW.cpu_L1i_cache_bytes IS NULL
      OR NEW.cpu_L2_cache_bytes IS NULL
      OR NEW.cpu_L3_cache_bytes IS NULL
      OR NEW.os_name IS NULL
      OR NEW.architecture_name IS NULL
    )
    THEN
      RAISE EXCEPTION 'None of the columns in "machine_view" can be NULL. '
        'all columns in table "gpu" will default to the empty string '''', '
        'as will blank "os.kernel_name". This is to allow uniqueness '
        'constraints to work. Thank you!.';
    END IF;

    SELECT public.get_cpu_id(
      NEW.cpu_model_name
      , NEW.cpu_core_count
      , NEW.cpu_thread_count
      , NEW.cpu_frequency_max_Hz
      , NEW.cpu_frequency_min_Hz
      , NEW.cpu_L1d_cache_bytes
      , NEW.cpu_L1i_cache_bytes
      , NEW.cpu_L2_cache_bytes
      , NEW.cpu_L3_cache_bytes
    ) INTO found_cpu_id;

    SELECT public.get_gpu_id(
      NEW.gpu_information
      , NEW.gpu_part_number
      , NEW.gpu_product_name
    ) INTO found_gpu_id;

    SELECT public.get_os_id(
      NEW.os_name
      , NEW.architecture_name
      , NEW.kernel_name
    ) INTO found_os_id;

    SELECT machine_id INTO result FROM public.machine AS m
    WHERE m.os_id = found_os_id
      AND m.cpu_id = found_cpu_id
      AND m.gpu_id = found_gpu_id
      AND m.machine_name = NEW.machine_name
      AND m.memory_bytes = NEW.memory_bytes
      AND m.cpu_actual_frequency_Hz = NEW.cpu_actual_frequency_Hz;

    IF result IS NOT NULL THEN
      -- row already exists
      RETURN NULL;
    ELSE
      INSERT INTO public.machine(
        os_id
        , cpu_id
        , gpu_id
        , machine_name
        , mac_address
        , memory_bytes
        , cpu_actual_frequency_Hz
        , machine_other_attributes
      )
      VALUES (
        found_os_id
        , found_cpu_id
        , found_gpu_id
        , NEW.machine_name
        , NEW.mac_address
        , NEW.memory_bytes
        , NEW.cpu_actual_frequency_Hz
        , NEW.machine_other_attributes
      )
      RETURNING machine_id INTO NEW.machine_id;
    END IF;

    RETURN NEW;
  END
$$
LANGUAGE plpgsql;

-- UNIT_VIEW_INSERT_ROW
CREATE OR REPLACE FUNCTION public.unit_view_insert_row()
RETURNS trigger AS
$$
  DECLARE
    found_benchmark_type_id integer;
    result integer;
  BEGIN
    IF (NEW.benchmark_type IS NULL OR NEW.units IS NULL)
    THEN
      RAISE EXCEPTION E'"benchmark_type" and "units" cannot be NULL.\n'
        'Further, if the "benchmark_type" has never been defined, '
        '"lessisbetter" must be defined or there will be an error.';
    END IF;

    -- It's OK for "lessisbetter" = NULL if "benchmark_type" already exists.
    SELECT public.get_benchmark_type_id(NEW.benchmark_type, NEW.lessisbetter)
      INTO found_benchmark_type_id;

    SELECT unit_id INTO result FROM public.unit AS u
    WHERE u.benchmark_type_id = found_benchmark_type_id
      AND u.units = NEW.units;

    IF result IS NOT NULL THEN
      -- row already exists
      RETURN NULL;
    ELSE
      INSERT INTO public.unit (
        benchmark_type_id
        , units
      )
      VALUES (
        found_benchmark_type_id
        , NEW.units
      )
      RETURNING unit_id INTO NEW.unit_id;
    END IF;

    RETURN NEW;
  END
$$
LANGUAGE plpgsql;

-- BENCHMARK_VIEW_INSERT_ROW
CREATE OR REPLACE FUNCTION public.benchmark_view_insert_row()
RETURNS trigger AS
$$
  DECLARE
    found_benchmark_language_id integer;
    found_units_id integer;
    result integer;
  BEGIN
    IF (
      NEW.benchmark_name IS NULL
      OR NEW.benchmark_version IS NULL
      OR NEW.benchmark_language IS NULL
      OR NEW.benchmark_type IS NULL
      OR NEW.benchmark_description IS NULL
      OR NEW.units IS NULL
    )
    THEN
      RAISE EXCEPTION 'The only nullable column in this view is '
        '"benchmark.parameter_names".';
    END IF;

    SELECT public.get_benchmark_language_id(
      NEW.benchmark_language
    ) INTO found_benchmark_language_id;

    SELECT public.get_unit_id(NEW.units) INTO found_units_id;

    SELECT benchmark_id INTO result FROM public.benchmark AS b
    WHERE b.benchmark_language_id = found_benchmark_language_id
      AND b.benchmark_name = NEW.benchmark_name
      -- handle nullable "parameter_names"
      AND b.parameter_names IS NOT DISTINCT FROM NEW.parameter_names
      AND b.benchmark_description = NEW.benchmark_description
      AND b.benchmark_version = NEW.benchmark_version
      AND b.unit_id = found_units_id;

    IF result IS NOT NULL THEN
      -- row already exists
      RETURN NULL;
    ELSE
      INSERT INTO public.benchmark(
        benchmark_language_id
        , benchmark_name
        , parameter_names
        , benchmark_description
        , benchmark_version
        , unit_id
      )
      VALUES (
        found_benchmark_language_id
        , NEW.benchmark_name
        , NEW.parameter_names
        , NEW.benchmark_description
        , NEW.benchmark_version
        , found_units_id
      )
      RETURNING benchmark_id INTO NEW.benchmark_id;
    END IF;

    RETURN NEW;
  END
$$
LANGUAGE plpgsql;

-- BENCHMARK_RUN_VIEW_INSERT_ROW
CREATE OR REPLACE FUNCTION public.benchmark_run_view_insert_row()
RETURNS trigger AS
$$
  DECLARE
    found_benchmark_id integer;
    found_benchmark_language_id integer;
    found_machine_id integer;
    found_environment_id integer;
    found_language_implementation_version_id integer;
  BEGIN
    IF (
      NEW.benchmark_name IS NULL
      OR NEW.benchmark_version IS NULL
      OR NEW.benchmark_language IS NULL
      OR NEW.value IS NULL
      OR NEW.run_timestamp IS NULL
      OR NEW.git_commit_timestamp IS NULL
      OR NEW.git_hash IS NULL
      OR NEW.language_implementation_version IS NULL
      OR NEW.mac_address IS NULL
    )
    THEN
      RAISE EXCEPTION 'Only the following columns can be NULL: '
        '"parameter_names", "val_min", "val_q1", "val_q3", "val_max".';
    END IF;

    SELECT public.get_benchmark_id(
      NEW.benchmark_language
      , NEW.benchmark_name
      , NEW.benchmark_version
    ) INTO found_benchmark_id;

    SELECT public.get_benchmark_language_id(
      NEW.benchmark_language
    ) INTO found_benchmark_language_id;

    SELECT public.get_machine_id(
      NEW.mac_address
    ) INTO found_machine_id;

    SELECT public.get_environment_id(
      NEW.benchmark_language
      , NEW.language_implementation_version
      , NEW.dependencies
    ) INTO found_environment_id;

    SELECT public.get_language_implementation_version_id(
      found_benchmark_language_id,
      NEW.language_implementation_version
    ) INTO found_language_implementation_version_id;

    INSERT INTO public.benchmark_run (
      parameter_values
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
      , machine_id
      , benchmark_language_id
      , language_implementation_version_id
      , environment_id
      , benchmark_id
    )
    VALUES (
      COALESCE(NEW.parameter_values, '{}'::jsonb)
      , NEW.value
      , NEW.git_commit_timestamp
      , NEW.git_hash
      , NEW.val_min
      , NEW.val_q1
      , NEW.val_q3
      , NEW.val_max
      , NEW.std_dev
      , NEW.n_obs
      , NEW.run_timestamp
      , NEW.run_metadata
      , NEW.run_notes
      , found_machine_id
      , found_benchmark_language_id
      , found_language_implementation_version_id
      , found_environment_id
      , found_benchmark_id
    ) returning benchmark_run_id INTO NEW.benchmark_run_id;

    RETURN NEW;
  END
$$
LANGUAGE plpgsql;

-- FULL_BENCHMARK_RUN_VIEW_INSERT_ROW
CREATE OR REPLACE FUNCTION public.full_benchmark_run_view_insert_row()
RETURNS trigger AS
$$
  DECLARE
    found_benchmark_id integer;
    found_benchmark_language_id integer;
    found_machine_id integer;
    found_environment_id integer;
    found_language_implementation_version_id integer;
  BEGIN
    IF (
      NEW.value IS NULL
      OR NEW.git_hash IS NULL
      OR NEW.git_commit_timestamp IS NULL
      OR NEW.run_timestamp IS NULL
      -- benchmark
      OR NEW.benchmark_name IS NULL
      OR NEW.benchmark_description IS NULL
      OR NEW.benchmark_version IS NULL
      OR NEW.benchmark_language IS NULL
      -- unit
      OR NEW.benchmark_type IS NULL
      OR NEW.units IS NULL
      OR NEW.lessisbetter IS NULL
      -- machine
      OR NEW.machine_name IS NULL
      OR NEW.memory_bytes IS NULL
      OR NEW.cpu_model_name IS NULL
      OR NEW.cpu_core_count IS NULL
      OR NEW.os_name IS NULL
      OR NEW.architecture_name IS NULL
      OR NEW.kernel_name IS NULL
      OR NEW.cpu_model_name IS NULL
      OR NEW.cpu_core_count IS NULL
      OR NEW.cpu_thread_count IS NULL
      OR NEW.cpu_frequency_max_Hz IS NULL
      OR NEW.cpu_frequency_min_Hz IS NULL
      OR NEW.cpu_L1d_cache_bytes IS NULL
      OR NEW.cpu_L1i_cache_bytes IS NULL
      OR NEW.cpu_L2_cache_bytes IS NULL
      OR NEW.cpu_L3_cache_bytes IS NULL
    )
    THEN
      RAISE EXCEPTION 'Only the following columns can be NULL: '
        '"machine_other_attributes", "parameter_names", "val_min", '
        '"val_q1", "val_q3", "val_max", "run_metadata", "run_notes". '
        'If "gpu_information", "gpu_part_number", "gpu_product_name", or '
        '"kernel_name" are null, they will be silently turned into an '
        'empty string ('''').';
    END IF;

    SELECT public.get_benchmark_id(
      NEW.benchmark_language
      , NEW.benchmark_name
      , NEW.parameter_names
      , NEW.benchmark_description
      , NEW.benchmark_version
      , NEW.benchmark_type
      , NEW.units
      , NEW.lessisbetter
    ) INTO found_benchmark_id;

    SELECT public.get_benchmark_language_id(
      NEW.benchmark_language
    ) INTO found_benchmark_language_id;

    SELECT public.get_machine_id(
      NEW.mac_address
      , NEW.machine_name
      , NEW.memory_bytes
      , NEW.cpu_actual_frequency_Hz
      -- os
      , NEW.os_name
      , NEW.architecture_name
      , NEW.kernel_name
      -- cpu
      , NEW.cpu_model_name
      , NEW.cpu_core_count
      , NEW.cpu_thread_count
      , NEW.cpu_frequency_max_Hz
      , NEW.cpu_frequency_min_Hz
      , NEW.cpu_L1d_cache_bytes
      , NEW.cpu_L1i_cache_bytes
      , NEW.cpu_L2_cache_bytes
      , NEW.cpu_L3_cache_bytes
      -- gpu
      , NEW.gpu_information
      , NEW.gpu_part_number
      , NEW.gpu_product_name
      -- nullable machine attributes
      , NEW.machine_other_attributes
    ) INTO found_machine_id;

    SELECT public.get_environment_id(
      NEW.benchmark_language
      , NEW.language_implementation_version
      , NEW.dependencies
    ) INTO found_environment_id;

    SELECT public.get_language_implementation_version_id(
      found_benchmark_language_id,
      NEW.language_implementation_version
    ) INTO found_language_implementation_version_id;

    INSERT INTO public.benchmark_run (
      parameter_values
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
      , machine_id
      , benchmark_language_id
      , language_implementation_version_id
      , environment_id
      , benchmark_id
    )
    VALUES (
      NEW.parameter_values
      , NEW.value
      , NEW.git_commit_timestamp
      , NEW.git_hash
      , NEW.val_min
      , NEW.val_q1
      , NEW.val_q3
      , NEW.val_max
      , NEW.std_dev
      , NEW.n_obs
      , NEW.run_timestamp
      , NEW.run_metadata
      , NEW.run_notes
      , found_machine_id
      , found_benchmark_language_id
      , found_language_implementation_version_id
      , found_environment_id
      , found_benchmark_id
    ) returning benchmark_run_id INTO NEW.benchmark_run_id;

    RETURN NEW;
  END
$$
LANGUAGE plpgsql;
