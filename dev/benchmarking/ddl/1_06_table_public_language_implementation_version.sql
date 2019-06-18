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


-- LANGUAGE_IMPLEMENTATION_VERSION
CREATE TABLE IF NOT EXISTS public.language_implementation_version
(
  language_implementation_version_id SERIAL
  , language_implementation_version citext NOT NULL DEFAULT ''
  , benchmark_language_id integer NOT NULL
  , PRIMARY KEY (language_implementation_version_id, benchmark_language_id)
  , FOREIGN KEY (benchmark_language_id) REFERENCES public.benchmark_language
);
COMMENT ON TABLE public.language_implementation_version
  IS 'The benchmark language implementation or compiler version, e.g. '
     '''CPython 2.7'' or ''PyPy x.y'' or ''gcc 7.3.0'' or '
     '''gcc (Ubuntu 7.3.0-27ubuntu1~18.04) 7.3.0''.';
COMMENT ON COLUMN public.language_implementation_version.language_implementation_version
  IS 'The version number used in the benchmark environment (e.g. ''2.7'').';

-- CONSTRAINTS
ALTER TABLE public.language_implementation_version
  ADD CONSTRAINT language_implementation_version_check_version_length
  CHECK (char_length(language_implementation_version) < 255);

CREATE UNIQUE INDEX language_implementation_version_unique_index
  ON public.language_implementation_version
    (benchmark_language_id, language_implementation_version);
COMMENT ON INDEX language_implementation_version_unique_index
  IS 'Enforce unique implementation versions for the languages.';
