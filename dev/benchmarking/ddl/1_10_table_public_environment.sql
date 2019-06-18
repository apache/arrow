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


-- ENVIRONMENT
CREATE TABLE IF NOT EXISTS public.environment
(
  environment_id SERIAL
  , language_implementation_version_id integer NOT NULL
  , benchmark_language_id integer NOT NULL
  , dependencies_id integer NOT NULL
  , PRIMARY KEY
      (environment_id, benchmark_language_id, language_implementation_version_id)
  , FOREIGN KEY
      (benchmark_language_id)
      REFERENCES public.benchmark_language
  , FOREIGN KEY
      (language_implementation_version_id, benchmark_language_id)
      REFERENCES public.language_implementation_version(
        language_implementation_version_id
        , benchmark_language_id
      )
  , FOREIGN KEY
      (dependencies_id)
      REFERENCES public.dependencies
);
COMMENT ON TABLE public.environment
  IS 'Identifies a build environment for a specific suite of benchmarks.';

-- CONSTRAINTS
CREATE UNIQUE INDEX environment_unique_index
  ON public.environment
    (benchmark_language_id, language_implementation_version_id, dependencies_id);
COMMENT ON INDEX environment_unique_index
  IS 'Enforce unique combinations of language version and dependencies.';
