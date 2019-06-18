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
---------------------------- ROLES ----------------------------
-- ARROW_WEB
CREATE ROLE arrow_web login password 'arrow';
COMMENT ON ROLE arrow_web IS 'Anonymous login user.';

-- ARROW_ADMIN
CREATE ROLE arrow_admin;
COMMENT ON ROLE arrow_admin
  IS 'Can select, insert, update, and delete on all public tables.';

-- ARROW_ANONYMOUS
CREATE ROLE arrow_anonymous;
COMMENT ON ROLE arrow_anonymous
  IS 'Can insert and select on all public tables.';

GRANT arrow_anonymous TO arrow_web;


---------------------------- PRIVILEGES ----------------------------
GRANT USAGE ON SCHEMA public TO arrow_anonymous, arrow_admin;

-- ARROW_ADMIN
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arrow_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public to arrow_admin;
GRANT SELECT, UPDATE, INSERT, DELETE ON ALL TABLES IN SCHEMA public
  TO arrow_admin;

-- ARROW_ANONYMOUS
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO arrow_anonymous;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO arrow_anonymous;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public to arrow_anonymous;
GRANT INSERT ON
  public.benchmark
  , public.benchmark_language
  , public.dependencies
  , public.language_implementation_version
  , public.benchmark_run
  , public.benchmark_type
  , public.cpu
  , public.environment
  , public.environment_view
  , public.gpu
  , public.machine
  , public.machine_view
  , public.os
  , public.unit
  --, public.project  -- The only disallowed table is `project`.
  , public.benchmark_run_view
  , public.benchmark_view
  , public.environment_view
  , public.full_benchmark_run_view
  , public.language_implementation_version_view
  , public.machine_view
  , public.unit_view
TO arrow_anonymous;
