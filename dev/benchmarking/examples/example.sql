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


-- Example insert into each of the views:
INSERT INTO public.project(project_name, project_url, repo_url)
VALUES (
  'Apache Arrow'
  , 'https://arrow.apache.org/'
  , 'https://github.com/apache/arrow');

INSERT INTO public.environment_view
  (benchmark_language, language_implementation_version, dependencies)
VALUES
  ('Python', 'CPython 2.7', '{"six": "", "numpy": "1.14", "other_lib": "1.0"}'),
  ('Python', 'CPython 3.6', '{"boost": "1.42", "numpy": "1.15"}');

INSERT INTO public.dependencies(dependencies)
VALUES
  ('{"boost": "1.68", "numpy": "1.14"}'),
  ('{"boost": "1.42", "numpy": "1.16"}');

INSERT INTO public.language_implementation_version_view
  (benchmark_language, language_implementation_version)
VALUES
  ('Python', 'CPython 2.7'),
  ('Python', 'CPython 3.6');

INSERT INTO public.unit_view
  (benchmark_type, units, lessisbetter)
VALUES
  ('Memory', 'gigabytes', True),
  ('Memory', 'kilobytes', True);


\echo 'use \\dv to list the views views';
\dv


SELECT * FROM environment_view;
SELECT * FROM unit_view;


INSERT INTO public.machine_view (
  mac_address
  , machine_name
  , memory_bytes
  , cpu_actual_frequency_hz
  , os_name
  , architecture_name
  , kernel_name
  , cpu_model_name
  , cpu_core_count
  , cpu_thread_count
  , cpu_frequency_max_hz
  , cpu_frequency_min_hz
  , cpu_l1d_cache_bytes
  , cpu_l1i_cache_bytes
  , cpu_l2_cache_bytes
  , cpu_l3_cache_bytes
  , machine_other_attributes
) VALUES (
  '08:00:2b:01:02:03'  -- mac_address
  , 'My-Machine-Name'  -- machine_name
  , 8589934592  -- memory_bytes
  -- All (?) standard mac address formats are allowable:
  -- https://www.postgresql.org/docs/11/datatype-net-types.html
  , 2300000000  -- cpu_actual_frequency_Hz
  , 'OSX'  -- os_name
  , 'x86_64'  -- architecture_name
  , '18.2.0'  -- kernel
  , 'Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz'  -- cpu_model_name
  , 2  -- cpu_core_count
  , 4  -- cpu_thread_count
  , 2300000000  -- cpu_frequency_max_Hz
  , 2300000000  -- cpu_frequency_min_Hz
  , 32768 -- cpu_l1d_cache_bytes
  , 32768  -- cpu_l1i_cache_bytes
  , 262144  -- cpu_l2_cache_bytes
  , 4194304  -- cpu_l3_cache_bytes
  , '{"example": "for machine_other_attributes"}'::jsonb
);


INSERT INTO public.full_benchmark_run_view (
  benchmark_name
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
  , cpu_actual_frequency_hz
  , os_name
  , architecture_name
  , kernel_name
  , cpu_model_name
  , cpu_core_count
  , cpu_thread_count
  , cpu_frequency_max_hz
  , cpu_frequency_min_hz
  , cpu_l1d_cache_bytes
  , cpu_l1i_cache_bytes
  , cpu_l2_cache_bytes
  , cpu_l3_cache_bytes
  , machine_other_attributes
  -- environment_view
  , benchmark_language
  , language_implementation_version
  , dependencies
) VALUES (
  'Benchmark 3'
  , '{"arg0"}'::text[]
  , 'Third benchmark'
  , 'Memory'
  , 'kilobytes'
  , TRUE
  , '0'
  -- datum
  , '{"arg0": 10}'::jsonb
  , 0.5
  , '2019-01-31 14:31:10 -0600'
  , '8136c46d5c60fb'
  , 0.5
  , 0.5
  , 0.5
  , 0.5
  , 0
  , 2
  , '2019-02-14 14:00:00 -0600'
  , '{"ci_99": [2.7e-06, 3.1e-06]}'::jsonb
  , 'Additional run_notes.'
  -- machine_view
  , 'My-Machine-Name'
  , '09-00-2c-01-02-03'
  , 8589934592
  , 2300000000
  , 'OSX'
  , 'x86_64'
  , '18.2.0'
  , 'Intel(R) Core(TM) i5-7360U CPU @ 2.30GHz'
  , 2
  , 4
  , 2300000000
  , 2300000000
  , 32768
  , 32768
  , 262144
  , 4194304
  , '{"example": "for machine_other_attributes"}'::jsonb
  -- environment_view
  , 'Python'
  , 'CPython 2.7'
  , '{"six": "", "numpy": "1.15", "other_lib": "1.0"}'::jsonb
);


-- Bulk load from CSV. First column is empty; serial "benchmark_run_id" will be assigned.
--\copy benchmark_run_view FROM 'examples/benchmark_run_example.csv' WITH (FORMAT csv, HEADER);

-- Load from JSON
--\set content `cat examples/benchmark_example.json`
--SELECT ingest_benchmark_view(:'content'::jsonb);

INSERT INTO public.benchmark_view (
    benchmark_name
    , parameter_names
    , benchmark_description
    , benchmark_type
    , units
    , lessisbetter
    , benchmark_version
    , benchmark_language
  ) VALUES (
    'Benchmark 1'
    , '{"arg0", "arg1", "arg2"}'::text[]
    , E'Description.\nNewlines are OK in a string escaped with leading "E".'
    , 'Time'
    , 'miliseconds'
    , TRUE
    , 'Hash of code or other way to identify distinct benchmark versions.'
    , 'Python'
  ), (
    'Benchmark 2'
    , '{"arg0", "arg1"}'::text[]
    , 'Description 2.'
    , 'Time'
    , 'nanoseconds'
    , TRUE
    , 'version 0'
    , 'Python'
  );


\x
SELECT * from benchmark_run_view;

\x
