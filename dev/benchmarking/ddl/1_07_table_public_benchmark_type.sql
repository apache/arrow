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


-- BENCHMARK_TYPE
CREATE TABLE IF NOT EXISTS public.benchmark_type
(
  benchmark_type_id SERIAL PRIMARY KEY
  , benchmark_type citext NOT NULL UNIQUE
  , lessisbetter boolean NOT NULL
);
COMMENT ON TABLE public.benchmark_type
  IS 'The type of benchmark. For example "time", "mem", "peakmem", "track"';
COMMENT ON COLUMN public.benchmark_type.benchmark_type
  IS 'The type of units, so ''time'' for seconds, miliseconds, or '
     '''mem'' for kilobytes, megabytes.';
COMMENT ON COLUMN public.benchmark_type.lessisbetter
  IS 'True if a smaller benchmark value is better.';

-- CONSTRAINTS
ALTER TABLE public.benchmark_type
  ADD CONSTRAINT benchmark_type_check_benchmark_type_char_length
  CHECK (char_length(benchmark_type) < 63);
