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


-- PROJECT_DETAILS
CREATE TYPE public.type_project_details AS (
  project_name text
  , project_url text
  , repo_url text
);

CREATE OR REPLACE FUNCTION public.project_details()
RETURNS public.type_project_details AS
$$
  SELECT project_name, project_url, repo_url
  FROM public.project
  ORDER BY last_changed DESC
  LIMIT 1
$$
LANGUAGE sql STABLE;
COMMENT ON FUNCTION public.project_details()
IS 'Get the current project name, url, and repo url.';


-------------------------- GET-OR-SET FUNCTIONS --------------------------
--  The following functions have the naming convention "get_<tablename>_id".
--  All of them attempt to SELECT the desired row given the column
--  values, and if it does not exist will INSERT it.
--
--  When functions are overloaded with fewer columns, it is to allow
--  selection only, given columns that comprise a unique index.

-- GET_CPU_ID
CREATE OR REPLACE FUNCTION public.get_cpu_id(
  cpu_model_name citext
  , cpu_core_count integer
  , cpu_thread_count integer
  , cpu_frequency_max_Hz bigint
  , cpu_frequency_min_Hz bigint
  , cpu_L1d_cache_bytes integer
  , cpu_L1i_cache_bytes integer
  , cpu_L2_cache_bytes integer
  , cpu_L3_cache_bytes integer
)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT cpu_id INTO result FROM public.cpu AS cpu
    WHERE cpu.cpu_model_name = $1
      AND cpu.cpu_core_count = $2
      AND cpu.cpu_thread_count = $3
      AND cpu.cpu_frequency_max_Hz = $4
      AND cpu.cpu_frequency_min_Hz = $5
      AND cpu.cpu_L1d_cache_bytes = $6
      AND cpu.cpu_L1i_cache_bytes = $7
      AND cpu.cpu_L2_cache_bytes = $8
      AND cpu.cpu_L3_cache_bytes = $9;

    IF result IS NULL THEN
      INSERT INTO public.cpu(
        cpu_model_name
        , cpu_core_count
        , cpu_thread_count
        , cpu_frequency_max_Hz
        , cpu_frequency_min_Hz
        , cpu_L1d_cache_bytes
        , cpu_L1i_cache_bytes
        , cpu_L2_cache_bytes
        , cpu_L3_cache_bytes
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING cpu_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_cpu_id(
  citext
  , integer
  , integer
  , bigint -- cpu_frequency_max_Hz
  , bigint -- cpu_frequency_min_Hz
  , integer
  , integer
  , integer
  , integer
)
IS 'Insert or select CPU data, returning "cpu.cpu_id".';

-- GET_GPU_ID
CREATE OR REPLACE FUNCTION public.get_gpu_id(
  gpu_information citext DEFAULT NULL
  , gpu_part_number citext DEFAULT NULL
  , gpu_product_name citext DEFAULT NULL
)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT gpu_id INTO result FROM public.gpu AS gpu
    WHERE
      gpu.gpu_information = COALESCE($1, '')
      AND gpu.gpu_part_number = COALESCE($2, '')
      AND gpu.gpu_product_name = COALESCE($3, '');

    IF result IS NULL THEN
      INSERT INTO public.gpu(
        gpu_information
        , gpu_part_number
        , gpu_product_name
      )
      VALUES (COALESCE($1, ''), COALESCE($2, ''), COALESCE($3, ''))
      RETURNING gpu_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_gpu_id(citext, citext, citext)
IS 'Insert or select GPU data, returning "gpu.gpu_id".';

-- GET_OS_ID
CREATE OR REPLACE FUNCTION public.get_os_id(
  os_name citext
  , architecture_name citext
  , kernel_name citext DEFAULT ''
)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT os_id INTO result FROM public.os AS os
    WHERE os.os_name = $1
      AND os.architecture_name = $2
      AND os.kernel_name = COALESCE($3, '');

    IF result is NULL THEN
      INSERT INTO public.os(os_name, architecture_name, kernel_name)
      VALUES ($1, $2, COALESCE($3, ''))
      RETURNING os_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_os_id(citext, citext, citext)
IS 'Insert or select OS data, returning "os.os_id".';

-- GET_MACHINE_ID (full signature)
CREATE OR REPLACE FUNCTION public.get_machine_id(
  mac_address macaddr
  , machine_name citext
  , memory_bytes bigint
  , cpu_actual_frequency_Hz bigint
  -- os
  , os_name citext
  , architecture_name citext
  , kernel_name citext
  -- cpu
  , cpu_model_name citext
  , cpu_core_count integer
  , cpu_thread_count integer
  , cpu_frequency_max_Hz bigint
  , cpu_frequency_min_Hz bigint
  , L1d_cache_bytes integer
  , L1i_cache_bytes integer
  , L2_cache_bytes integer
  , L3_cache_bytes integer
  -- gpu
  , gpu_information citext DEFAULT ''
  , gpu_part_number citext DEFAULT NULL
  , gpu_product_name citext DEFAULT NULL
  -- nullable machine attributes
  , machine_other_attributes jsonb DEFAULT NULL
)
RETURNS integer AS
$$
  DECLARE
    found_cpu_id integer;
    found_gpu_id integer;
    found_os_id integer;
    result integer;
  BEGIN
    -- Can't bypass looking up all the values because of unique constraint.
    SELECT public.get_cpu_id(
      cpu_model_name
      , cpu_core_count
      , cpu_thread_count
      , cpu_frequency_max_Hz
      , cpu_frequency_min_Hz
      , L1d_cache_bytes
      , L1i_cache_bytes
      , L2_cache_bytes
      , L3_cache_bytes
    ) INTO found_cpu_id;

    SELECT public.get_gpu_id(
      gpu_information
      , gpu_part_number
      , gpu_product_name
    ) INTO found_gpu_id;

    SELECT public.get_os_id(
      os_name
      , architecture_name
      , kernel_name
    ) INTO found_os_id;

    SELECT machine_id INTO result FROM public.machine AS m
    WHERE m.os_id = found_os_id
      AND m.cpu_id = found_cpu_id
      AND m.gpu_id = found_gpu_id
      AND m.mac_address = $1
      AND m.machine_name = $2
      AND m.memory_bytes = $3
      AND m.cpu_actual_frequency_Hz = $4;

    IF result IS NULL THEN
      INSERT INTO public.machine(
        os_id
        , cpu_id
        , gpu_id
        , mac_address
        , machine_name
        , memory_bytes
        , cpu_actual_frequency_Hz
        , machine_other_attributes
      )
      VALUES (found_os_id, found_cpu_id, found_gpu_id, $1, $2, $3, $4, $20)
      RETURNING machine_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_machine_id(
  macaddr
  , citext
  , bigint  -- memory_bytes
  , bigint  -- cpu_frequency_actual_Hz
  -- os
  , citext
  , citext
  , citext
  -- cpu
  , citext
  , integer
  , integer
  , bigint  -- cpu_frequency_max_Hz
  , bigint  -- cpu_frequency_min_Hz
  , integer
  , integer
  , integer
  , integer
  -- gpu
  , citext
  , citext
  , citext
  -- nullable machine attributes
  , jsonb
)
IS 'Insert or select machine data, returning "machine.machine_id".';

-- GET_MACHINE_ID (given unique mac_address)
CREATE OR REPLACE FUNCTION public.get_machine_id(mac_address macaddr)
RETURNS integer AS
$$
  SELECT machine_id FROM public.machine AS m
  WHERE m.mac_address = $1;
$$
LANGUAGE sql STABLE;
COMMENT ON FUNCTION public.get_machine_id(macaddr)
IS 'Select machine_id given its mac address, returning "machine.machine_id".';

-- GET_BENCHMARK_LANGUAGE_ID
CREATE OR REPLACE FUNCTION public.get_benchmark_language_id(language citext)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT benchmark_language_id INTO result
    FROM public.benchmark_language AS bl
    WHERE bl.benchmark_language = language;

    IF result IS NULL THEN
      INSERT INTO public.benchmark_language(benchmark_language)
      VALUES (language)
      RETURNING benchmark_language_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_benchmark_language_id(citext)
IS 'Insert or select benchmark_language returning '
   '"benchmark_language.benchmark_language_id".';

-- GET_LANGUAGE_IMPLEMENTATION_VERSION_ID
CREATE OR REPLACE FUNCTION public.get_language_implementation_version_id(
  language citext
  , language_implementation_version citext DEFAULT ''
)
RETURNS integer AS
$$
  DECLARE
    language_id integer;
    result integer;
  BEGIN
    SELECT public.get_benchmark_language_id($1) INTO language_id;

    SELECT language_implementation_version_id INTO result FROM public.language_implementation_version AS lv
    WHERE lv.benchmark_language_id = language_id
      AND lv.language_implementation_version = COALESCE($2, '');

    IF result IS NULL THEN
      INSERT INTO
        public.language_implementation_version(benchmark_language_id, language_implementation_version)
      VALUES (language_id, COALESCE($2, ''))
      RETURNING language_implementation_version_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_language_implementation_version_id(citext, citext)
IS 'Insert or select language and version data, '
      'returning "language_implementation_version.language_implementation_version_id".';

CREATE OR REPLACE FUNCTION public.get_language_implementation_version_id(
  -- overload for when language_id is known
  language_id integer
  , language_implementation_version citext DEFAULT ''
)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT language_implementation_version_id INTO result FROM public.language_implementation_version AS lv
    WHERE lv.benchmark_language_id = language_id
      AND lv.language_implementation_version = COALESCE($2, '');

    IF result IS NULL THEN
      INSERT INTO
        public.language_implementation_version(benchmark_language_id, language_implementation_version)
      VALUES (language_id, COALESCE($2, ''))
      RETURNING language_implementation_version_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;

-- GET_LANGUAGE_DEPENDENCY_LOOKUP_ID
CREATE OR REPLACE FUNCTION public.get_dependencies_id(
  dependencies jsonb DEFAULT '{}'::jsonb
)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT dependencies_id INTO result
    FROM public.dependencies AS ldl
    WHERE ldl.dependencies = COALESCE($1, '{}'::jsonb);

    IF result IS NULL THEN
      INSERT INTO
        public.dependencies(dependencies)
      VALUES (COALESCE($1, '{}'::jsonb))
      RETURNING dependencies_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_dependencies_id(jsonb)
IS 'Insert or select dependencies, returning "dependencies.dependencies_id".';

-- GET_ENVIRONMENT_ID
CREATE OR REPLACE FUNCTION public.get_environment_id(
  language citext,
  language_implementation_version citext DEFAULT '',
  dependencies jsonb DEFAULT '{}'::jsonb
)
RETURNS integer AS
$$
  DECLARE
    found_language_id integer;
    found_version_id integer;
    found_dependencies_id integer;
    result integer;
  BEGIN
    SELECT public.get_benchmark_language_id($1) INTO found_language_id;
    SELECT
      public.get_language_implementation_version_id(found_language_id, $2)
      INTO found_version_id;
    SELECT
      public.get_dependencies_id ($3)
      INTO found_dependencies_id;

    SELECT environment_id INTO result FROM public.environment AS e
    WHERE e.benchmark_language_id = found_language_id
      AND e.language_implementation_version_id = found_version_id
      AND e.dependencies_id = found_dependencies_id;

    IF result IS NULL THEN
      INSERT INTO
        public.environment(
          benchmark_language_id
          , language_implementation_version_id
          , dependencies_id
        )
      VALUES (found_language_id, found_version_id, found_dependencies_id)
      RETURNING environment_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_environment_id(citext, citext, jsonb)
IS 'Insert or select language, language version, and dependencies, '
      'returning "environment.environment_id".';

-- GET_BENCHMARK_TYPE_ID (full signature)
CREATE OR REPLACE FUNCTION public.get_benchmark_type_id(
  benchmark_type citext
  , lessisbetter boolean
)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT benchmark_type_id INTO result FROM public.benchmark_type AS bt
    WHERE bt.benchmark_type = $1
      AND bt.lessisbetter = $2;

    IF result IS NULL THEN
      INSERT INTO public.benchmark_type(benchmark_type, lessisbetter)
      VALUES($1, $2)
      RETURNING benchmark_type_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_benchmark_type_id(citext, boolean)
IS 'Insert or select benchmark type and lessisbetter, '
   'returning "benchmark_type.benchmark_type_id".';

-- GET_BENCHMARK_TYPE_ID (given unique benchmark_type string only)
CREATE OR REPLACE FUNCTION public.get_benchmark_type_id(
  benchmark_type citext
)
RETURNS integer AS
$$
  DECLARE
    result integer;
  BEGIN
    SELECT benchmark_type_id INTO result FROM public.benchmark_type AS bt
    WHERE bt.benchmark_type = $1;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_benchmark_type_id(citext)
IS 'Select benchmark_type_id given benchmark type (e.g. ''time''), '
   'returning "benchmark_type.benchmark_type_id".';

-- GET_UNIT_ID (full signature)
CREATE OR REPLACE FUNCTION public.get_unit_id(
  benchmark_type citext
  , units citext
  , lessisbetter boolean DEFAULT NULL
)
RETURNS integer AS
$$
  DECLARE
    found_benchmark_type_id integer;
    result integer;
  BEGIN
  
    IF ($3 IS NOT NULL)  -- if lessisbetter is not null
    THEN
      SELECT public.get_benchmark_type_id($1, $3)
        INTO found_benchmark_type_id;
    ELSE
      SELECT public.get_benchmark_type_id($1)
        INTO found_benchmark_type_id;
    END IF;

    SELECT unit_id INTO result FROM public.unit AS u
    WHERE u.benchmark_type_id = found_benchmark_type_id
      AND u.units = $2;

    IF result IS NULL THEN
      INSERT INTO public.unit(benchmark_type_id, units)
      VALUES(found_benchmark_type_id, $2)
      RETURNING unit_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_unit_id(citext, citext, boolean)
IS 'Insert or select benchmark type (e.g. ''time''), '
   'units string (e.g. ''miliseconds''), '
   'and "lessisbetter" (true if smaller benchmark values are better), '
   'returning "unit.unit_id".';

-- GET_UNIT_ID (given unique units string only)
CREATE OR REPLACE FUNCTION public.get_unit_id(units citext)
RETURNS integer AS
$$
  SELECT unit_id FROM public.unit AS u
  WHERE u.units = units;
$$
LANGUAGE sql STABLE;
COMMENT ON FUNCTION public.get_unit_id(citext)
IS 'Select unit_id given unit name, returning "unit.unit_id".';

-- GET_BENCHMARK_ID (full signature)
CREATE OR REPLACE FUNCTION public.get_benchmark_id(
  benchmark_language citext
  , benchmark_name citext
  , parameter_names text[]
  , benchmark_description text
  , benchmark_version citext
  , benchmark_type citext
  , units citext
  , lessisbetter boolean
)
RETURNS integer AS
$$
  DECLARE
    found_benchmark_language_id integer;
    found_unit_id integer;
    result integer;
  BEGIN
    SELECT public.get_benchmark_language_id(
      benchmark_language
    ) INTO found_benchmark_language_id;

    SELECT public.get_unit_id(
      benchmark_type
      , units
      , lessisbetter
    ) INTO found_unit_id;

    SELECT benchmark_id INTO result FROM public.benchmark AS b
    WHERE b.benchmark_language_id = found_benchmark_language_id
      AND b.benchmark_name = $2
      -- handle nullable "parameter_names"
      AND b.parameter_names IS NOT DISTINCT FROM $3
      AND b.benchmark_description = $4
      AND b.benchmark_version = $5
      AND b.unit_id = found_unit_id;

    IF result IS NULL THEN
      INSERT INTO public.benchmark(
        benchmark_language_id
        , benchmark_name
        , parameter_names
        , benchmark_description
        , benchmark_version
        , unit_id
      )
      VALUES (found_benchmark_language_id, $2, $3, $4, $5, found_unit_id)
      RETURNING benchmark_id INTO result;
    END IF;

    RETURN result;
  END
$$
LANGUAGE plpgsql;
COMMENT ON FUNCTION public.get_benchmark_id(
  citext
  , citext
  , text[]
  , text
  , citext
  , citext
  , citext
  , boolean
)
IS 'Insert/select benchmark given data, returning "benchmark.benchmark_id".';

-- GET_BENCHMARK_ID (by unique columns)
CREATE OR REPLACE FUNCTION public.get_benchmark_id(
  benchmark_language citext
  , benchmark_name citext
  , benchmark_version citext
)
RETURNS integer AS
$$
  WITH language AS (
    SELECT public.get_benchmark_language_id(benchmark_language) AS id
  )
  SELECT b.benchmark_id
  FROM public.benchmark AS b
  JOIN language ON b.benchmark_language_id = language.id
  WHERE b.benchmark_name = benchmark_name
    AND benchmark_version = benchmark_version
$$
LANGUAGE sql STABLE;
COMMENT ON FUNCTION public.get_benchmark_id(citext, citext, citext)
IS 'Select existing benchmark given unique columns, '
   'returning "benchmark.benchmark_id".';
