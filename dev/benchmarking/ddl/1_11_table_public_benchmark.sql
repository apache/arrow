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


-- BENCHMARK
CREATE TABLE IF NOT EXISTS public.benchmark
(
  benchmark_id SERIAL
  , benchmark_name citext NOT NULL
  , parameter_names text[]
  , benchmark_description text NOT NULL
  , benchmark_version citext NOT NULL
  , unit_id integer NOT NULL
  , benchmark_language_id integer NOT NULL
  , PRIMARY KEY (benchmark_id, benchmark_language_id)
  , FOREIGN KEY (benchmark_language_id) REFERENCES public.benchmark_language
  , FOREIGN KEY (unit_id) REFERENCES public.unit
);
COMMENT ON TABLE public.benchmark
  IS 'Identifies an individual benchmark.';
COMMENT ON COLUMN public.benchmark.parameter_names
  IS 'A list of strings identifying the parameter names in the benchmark.';
COMMENT ON COLUMN public.benchmark.benchmark_version
  IS 'Can be any string. In Airspeed Velocity, the version is '
     'by default the hash of the entire code string for the benchmark.';

-- CONSTRAINTS
CREATE INDEX benchmark_index_on_benchmark_language_id
  ON public.benchmark(benchmark_language_id);

CREATE INDEX benchmark_index_on_unit_id
  ON public.benchmark(unit_id);

CREATE UNIQUE INDEX benchmark_unique_index_on_language_benchmark_version
  ON public.benchmark
    (benchmark_language_id, benchmark_name, benchmark_version);
COMMENT ON INDEX public.benchmark_unique_index_on_language_benchmark_version
  IS 'Enforce uniqueness of benchmark name and version for a given language.';
