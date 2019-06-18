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


-------------------------- IMPORT HELPERS  --------------------------
-- Load from JSON (from https://stackoverflow.com/a/48396608)
-- How to use it in the psql client:
--   \set content `cat /examples/machine.json`
--   select ingest_machine(:'content'::jsonb);
-- INGEST_MACHINE_VIEW
CREATE OR REPLACE FUNCTION public.ingest_machine_view(from_jsonb jsonb)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    INSERT INTO public.machine_view
    SELECT * FROM jsonb_populate_record(null::public.machine_view, from_jsonb)
    RETURNING machine_id INTO result;
    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.ingest_machine_view(jsonb) IS
  E'The argument is a JSON object. NOTE: key names must be entirely\n'
  'lowercase, or the insert will fail. Extra key-value pairs are ignored.\n'
  'Example::\n\n'
  '  {\n'
  '    "mac_address": "0a:00:2d:01:02:03",\n'
  '    "machine_name": "Yet-Another-Machine-Name",\n'
  '    "memory_bytes": 8589934592,\n'
  '    "cpu_actual_frequency_hz": 2300000000,\n'
  '    "os_name": "OSX",\n'
  '    "architecture_name": "x86_64",\n'
  '    "kernel_name": "18.2.0",\n'
  '    "cpu_model_name": "Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz",\n'
  '    "cpu_core_count": 2,\n'
  '    "cpu_thread_count": 4,\n'
  '    "cpu_frequency_max_hz": 2300000000,\n'
  '    "cpu_frequency_min_hz": 2300000000,\n'
  '    "cpu_l1d_cache_bytes": 32768,\n'
  '    "cpu_l1i_cache_bytes": 32768,\n'
  '    "cpu_l2_cache_bytes": 262144,\n'
  '    "cpu_l3_cache_bytes": 4194304,\n'
  '    "machine_other_attributes": {"just": "an example"},\n'
  '    "gpu_information": "",\n'
  '    "gpu_part_number": "",\n'
  '    "gpu_product_name": ""\n'
  '  }\n\n'
  'To identify which columns in "machine_view" are required,\n'
  'please see the view documentation in :ref:`benchmark-data-model`.\n';

-- INGEST_BENCHMARK_VIEW
CREATE OR REPLACE FUNCTION public.ingest_benchmark_view(from_jsonb jsonb)
RETURNS setof integer AS
$$
  BEGIN
    RETURN QUERY
    INSERT INTO public.benchmark_view
    SELECT * FROM jsonb_populate_recordset(
      null::public.benchmark_view
      , from_jsonb
    )
    RETURNING benchmark_id;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.ingest_benchmark_view(jsonb) IS
  E'The argument is a JSON object. NOTE: key names must be entirely\n'
  'lowercase, or the insert will fail. Extra key-value pairs are ignored.\n'
  'Example::\n\n'
  '  [\n'
  '    {\n'
  '      "benchmark_name": "Benchmark 1",\n'
  '      "parameter_names": ["arg0", "arg1", "arg2"],\n'
  '      "benchmark_description": "First benchmark",\n'
  '      "benchmark_type": "Time",\n'
  '      "units": "miliseconds",\n'
  '      "lessisbetter": true,\n'
  '      "benchmark_version": "second version",\n'
  '      "benchmark_language": "Python"\n'
  '    },\n'
  '    {\n'
  '      "benchmark_name": "Benchmark 2",\n'
  '      "parameter_names": ["arg0", "arg1"],\n'
  '      "benchmark_description": "Description 2.",\n'
  '      "benchmark_type": "Time",\n'
  '      "units": "nanoseconds",\n'
  '      "lessisbetter": true,\n'
  '      "benchmark_version": "second version",\n'
  '      "benchmark_language": "Python"\n'
  '    }\n'
  '  ]\n\n'
  'To identify which columns in "benchmark_view" are required,\n'
  'please see the view documentation in :ref:`benchmark-data-model`.\n';

-- INGEST_BENCHMARK_RUN_VIEW
CREATE OR REPLACE FUNCTION public.ingest_benchmark_run_view(from_jsonb jsonb)
RETURNS setof bigint AS
$$
  BEGIN
    RETURN QUERY
    INSERT INTO public.benchmark_run_view
    SELECT * FROM
    jsonb_populate_recordset(null::public.benchmark_run_view, from_jsonb)
    RETURNING benchmark_run_id;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.ingest_benchmark_run_view(jsonb) IS
  E'The argument is a JSON object. NOTE: key names must be entirely\n'
  'lowercase, or the insert will fail. Extra key-value pairs are ignored.\n'
  'Example::\n\n'
  '  [\n'
  '    {\n'
  '      "benchmark_name": "Benchmark 2",\n'
  '      "benchmark_version": "version 0",\n'
  '      "parameter_values": {"arg0": 100, "arg1": 5},\n'
  '      "value": 2.5,\n'
  '      "git_commit_timestamp": "2019-02-08 22:35:53 +0100",\n'
  '      "git_hash": "324d3cf198444a",\n'
  '      "val_min": 1,\n'
  '      "val_q1": 2,\n'
  '      "val_q3": 3,\n'
  '      "val_max": 4,\n'
  '      "std_dev": 1.41,\n'
  '      "n_obs": 8,\n'
  '      "run_timestamp": "2019-02-14 03:00:05 -0600",\n'
  '      "mac_address": "08:00:2b:01:02:03",\n'
  '      "benchmark_language": "Python",\n'
  '      "language_implementation_version": "CPython 2.7",\n'
  '      "dependencies": {"six": "", "numpy": "1.14", "other_lib": "1.0"}\n'
  '    },\n'
  '    {\n'
  '      "benchmark_name": "Benchmark 2",\n'
  '      "benchmark_version": "version 0",\n'
  '      "parameter_values": {"arg0": 1000, "arg1": 5},\n'
  '      "value": 5,\n'
  '      "git_commit_timestamp": "2019-02-08 22:35:53 +0100",\n'
  '      "git_hash": "324d3cf198444a",\n'
  '      "std_dev": 3.14,\n'
  '      "n_obs": 8,\n'
  '      "run_timestamp": "2019-02-14 03:00:10 -0600",\n'
  '      "mac_address": "08:00:2b:01:02:03",\n'
  '      "benchmark_language": "Python",\n'
  '      "language_implementation_version": "CPython 2.7",\n'
  '      "dependencies": {"six": "", "numpy": "1.14", "other_lib": "1.0"}\n'
  '    }\n'
  '  ]\n'
  'To identify which columns in "benchmark_run_view" are required,\n'
  'please see the view documentation in :ref:`benchmark-data-model`.\n';

-- INGEST_BENCHMARK_RUNS_WITH_CONTEXT
CREATE OR REPLACE FUNCTION public.ingest_benchmark_runs_with_context(from_jsonb jsonb)
RETURNS setof bigint AS
$$
  DECLARE
    context_jsonb jsonb;
    found_environment_id integer;
    found_machine_id integer;
  BEGIN
    SELECT from_jsonb -> 'context' INTO context_jsonb;

    SELECT public.get_machine_id((context_jsonb ->> 'mac_address')::macaddr)
    INTO found_machine_id;

    SELECT get_environment_id(
      (context_jsonb ->> 'benchmark_language')::citext
      , (context_jsonb ->> 'language_implementation_version')::citext
      , context_jsonb -> 'dependencies'
    ) INTO found_environment_id;

    RETURN QUERY
    WITH run_datum AS (
      SELECT *
      FROM jsonb_to_recordset(from_jsonb -> 'benchmarks')
      AS x(
        benchmark_name citext
        , parameter_values jsonb
        , value numeric
        , val_min numeric
        , val_q1 numeric
        , val_q3 numeric
        , val_max numeric
        , std_dev numeric
        , n_obs integer
        , run_timestamp timestamp (0) with time zone
        , run_metadata jsonb
        , run_notes text
      )
    ), benchmark_name_and_id AS (
      SELECT
        key AS benchmark_name
        , public.get_benchmark_id(
            (context_jsonb ->> 'benchmark_language')::citext
            , key::citext  -- benchmark_name
            , value::citext  -- benchmark_version
        ) AS benchmark_id
      FROM jsonb_each_text(from_jsonb -> 'benchmark_version')
    )
    INSERT INTO public.benchmark_run (
      benchmark_id
      -- run_datum
      , parameter_values
      , value
      , val_min
      , val_q1
      , val_q3
      , val_max
      , std_dev
      , n_obs
      , run_metadata
      , run_notes
      -- additional context information
      , git_commit_timestamp
      , git_hash
      , run_timestamp
      -- machine
      , machine_id
      -- environment
      , environment_id
      , language_implementation_version_id
      , benchmark_language_id
    )
    SELECT
      b.benchmark_id
      -- run_datum
      , run_datum.parameter_values
      , run_datum.value
      , run_datum.val_min
      , run_datum.val_q1
      , run_datum.val_q3
      , run_datum.val_max
      , run_datum.std_dev
      , run_datum.n_obs
      , run_datum.run_metadata
      , run_datum.run_notes
      -- additional context information
      , (context_jsonb ->> 'git_commit_timestamp')::timestamp (0) with time zone
      , context_jsonb ->> 'git_hash'
      , (context_jsonb ->> 'run_timestamp')::timestamp (0) with time zone
      -- machine
      , found_machine_id
      -- environment
      , e.environment_id
      , e.language_implementation_version_id
      , e.benchmark_language_id
    FROM run_datum
    JOIN public.environment AS e
      ON e.environment_id = found_environment_id
    JOIN benchmark_name_and_id AS b
      ON b.benchmark_name = run_datum.benchmark_name
    RETURNING benchmark_run_id;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.ingest_benchmark_runs_with_context(jsonb) IS
  E'The argument is a JSON object. NOTE: key names must be entirely\n'
  'lowercase, or the insert will fail. Extra key-value pairs are ignored.\n'
  'The object contains three key-value pairs::\n\n'
  '    {"context": {\n'
  '        "mac_address": "08:00:2b:01:02:03",\n'
  '        "benchmark_language": "Python",\n'
  '        "language_implementation_version": "CPython 3.6",\n'
  '        "dependencies": {"six": "", "numpy": "1.14", "other_lib": "1.0"},\n'
  '        "git_commit_timestamp": "2019-02-14 22:42:22 +0100",\n'
  '        "git_hash": "123456789abcde",\n'
  '        "run_timestamp": "2019-02-14 03:00:40 -0600",\n'
  '        "extra stuff": "does not hurt anything and will not be added."\n'
  '      },\n'
  '     "benchmark_version": {\n'
  '        "Benchmark Name 1": "Any string can be a version.",\n'
  '        "Benchmark Name 2": "A git hash can be a version.",\n'
  '        "An Unused Benchmark Name": "Will be ignored."\n'
  '      },\n'
  '     "benchmarks": [\n'
  '        {\n'
  '          "benchmark_name": "Benchmark Name 1",\n'
  '          "parameter_values": {"argument1": 1, "argument2": "value2"},\n'
  '          "value": 42,\n'
  '          "val_min": 41.2,\n'
  '          "val_q1":  41.5,\n'
  '          "val_q3":  42.5,\n'
  '          "val_max": 42.8,\n'
  '          "std_dev": 0.5,\n'
  '          "n_obs": 100,\n'
  '          "run_metadata": {"any": "key-value pairs"},\n'
  '          "run_notes": "Any relevant notes."\n'
  '        },\n'
  '        {\n'
  '          "benchmark_name": "Benchmark Name 2",\n'
  '          "parameter_values": {"not nullable": "Use {} if no params."},\n'
  '          "value": 8,\n'
  '          "std_dev": 1,\n'
  '          "n_obs": 2,\n'
  '        }\n'
  '      ]\n'
  '    }\n\n'
  '- The entry for "context" contains the machine, environment, and timestamp\n'
  '  information common to all of the runs\n'
  '- The entry for "benchmark_version" maps benchmark\n'
  '  names to their version strings. (Which can be a git hash,\n'
  '  the entire code string, a number, or any other string of your choice.)\n'
  '- The entry for "benchmarks" is a list of benchmark run data\n'
  '  for the given context and benchmark versions. The first example\n'
  '  benchmark run entry contains all possible values, even\n'
  '  nullable ones, and the second entry omits all nullable values.\n\n';
