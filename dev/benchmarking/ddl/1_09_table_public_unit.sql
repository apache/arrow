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


-- UNIT
CREATE TABLE IF NOT EXISTS public.unit
(
  unit_id SERIAL PRIMARY KEY
  , units citext NOT NULL UNIQUE
  , benchmark_type_id integer NOT NULL
  , FOREIGN KEY (benchmark_type_id)
    REFERENCES public.benchmark_type(benchmark_type_id)
);
COMMENT ON TABLE public.unit IS 'The actual units for a reported benchmark.';
COMMENT ON COLUMN public.unit.units
  IS 'For example: nanoseconds, microseconds, bytes, megabytes.';

-- CONSTRAINTS
ALTER TABLE public.unit
  ADD CONSTRAINT unit_check_units_string_length
  CHECK (char_length(units) < 63);
