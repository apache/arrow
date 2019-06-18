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


-- BENCHMARK_LANGUAGE
CREATE TABLE IF NOT EXISTS public.benchmark_language
(
  benchmark_language_id SERIAL PRIMARY KEY
  , benchmark_language citext NOT NULL UNIQUE
);
COMMENT ON TABLE public.benchmark_language
  IS 'The language the benchmark was written in (and presumably for).';
COMMENT ON COLUMN public.benchmark_language.benchmark_language
  IS 'The benchmark language. For example: Python';

-- CONSTRAINTS
ALTER TABLE public.benchmark_language
  ADD CONSTRAINT benchmark_language_check_language_length
  CHECK (char_length(benchmark_language) < 63);
