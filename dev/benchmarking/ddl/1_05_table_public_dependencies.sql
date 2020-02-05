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


-- DEPENDENCIES
CREATE TABLE IF NOT EXISTS public.dependencies
(
  dependencies_id SERIAL PRIMARY KEY
  , dependencies jsonb UNIQUE NOT NULL DEFAULT '{}'::jsonb
);
COMMENT ON TABLE public.dependencies
  IS E'@name dependencies.\n'
      'A JSON object mapping dependencies to their versions.';
COMMENT ON COLUMN public.dependencies.dependencies
  IS 'For example: ''{"boost": "1.69", "conda": "", "numpy": "1.15"}''.';
