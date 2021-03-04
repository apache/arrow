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


-- OS
CREATE TABLE IF NOT EXISTS public.os
(
  os_id SERIAL PRIMARY KEY
  , os_name citext NOT NULL
  , architecture_name citext NOT NULL
  , kernel_name citext NOT NULL DEFAULT ''
);
-- @name os. forces retention of an 's' in the Graphile GraphQL api.
COMMENT ON TABLE public.os
  IS E'@name os.\nOperating system name and kernel (version).';
COMMENT ON COLUMN public.os.os_name
  IS 'Operating system name. For example, OSX, Ubuntu, Windows`.';
COMMENT ON COLUMN public.os.architecture_name
  IS 'Operating system architecture; the output of `uname -m`.';
COMMENT ON COLUMN public.os.kernel_name
  IS 'Operating system kernel, or NULL. '
     'On Linux/OSX, the output of `uname -r`. '
     'On Windows, the output of `ver`.';

-- CONSTRAINTS
ALTER TABLE public.os
  ADD CONSTRAINT os_check_os_name_length
  CHECK (char_length(os_name) < 63);

ALTER TABLE public.os
  ADD CONSTRAINT os_check_architecture_name_length
  CHECK (char_length(architecture_name) < 63);

ALTER TABLE public.os
  ADD CONSTRAINT os_check_kernel_name_length
  CHECK (char_length(kernel_name) < 63);

CREATE UNIQUE INDEX os_unique_index
  ON public.os(os_name, architecture_name, kernel_name);
COMMENT ON INDEX public.os_unique_index
  IS 'Enforce uniqueness of os, architecture, and kernel names.';
