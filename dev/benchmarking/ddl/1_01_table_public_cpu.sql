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


-- CPU
CREATE TABLE IF NOT EXISTS public.cpu
(
  cpu_id SERIAL PRIMARY KEY
  , cpu_model_name citext NOT NULL UNIQUE
  , cpu_core_count integer NOT NULL
  , cpu_thread_count integer NOT NULL
  , cpu_frequency_max_Hz bigint NOT NULL
  , cpu_frequency_min_Hz bigint NOT NULL
  , cpu_L1d_cache_bytes integer NOT NULL
  , cpu_L1i_cache_bytes integer NOT NULL
  , cpu_L2_cache_bytes integer NOT NULL
  , cpu_L3_cache_bytes integer NOT NULL
);
COMMENT ON TABLE public.cpu
  IS 'CPU model and its specifications.';
COMMENT ON COLUMN public.cpu.cpu_id
  IS 'The primary key for the CPU table. '
     'NOTE: This is a synthetic primary key and not meant to represent a '
     'processor instruction to read capabilities.';
COMMENT ON COLUMN public.cpu.cpu_model_name
  IS 'The output of `sysctl -n machdep.cpu.brand_stringp`.';
COMMENT ON COLUMN public.cpu.cpu_core_count
  IS 'The output of `sysctl -n hw.physicalcpu`.';
COMMENT ON COLUMN public.cpu.cpu_thread_count
  IS 'The output of `sysctl -n hw.logicalcpu`.';
COMMENT ON COLUMN public.cpu.cpu_frequency_max_Hz
  IS 'The output of `sysctl -n hw.cpufrequency_max`.';
COMMENT ON COLUMN public.cpu.cpu_frequency_min_Hz
  IS 'The output of `sysctl -n hw.cpufrequency_min`.';
COMMENT ON COLUMN public.cpu.cpu_L1d_cache_bytes
  IS 'The output of `sysctl -n hw.l1dcachesize`.';
COMMENT ON COLUMN public.cpu.cpu_L1i_cache_bytes
  IS 'The output of `sysctl -n hw.l1icachesize`.';
COMMENT ON COLUMN public.cpu.cpu_L2_cache_bytes
  IS 'The output of `sysctl -n hw.l2cachesize`.';
COMMENT ON COLUMN public.cpu.cpu_L3_cache_bytes
  IS 'The output of `sysctl -n hw.l3cachesize`.';

-- CONSTRAINTS
ALTER TABLE public.cpu
  ADD CONSTRAINT cpu_check_cpu_model_name_length
  CHECK (char_length(cpu_model_name) < 255);
