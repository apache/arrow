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


-- GPU
CREATE TABLE IF NOT EXISTS public.gpu
(
  gpu_id SERIAL PRIMARY KEY
  , gpu_information citext UNIQUE NOT NULL DEFAULT ''
  , gpu_part_number citext NOT NULL DEFAULT ''
  , gpu_product_name citext NOT NULL DEFAULT ''
);
COMMENT ON TABLE public.gpu IS 'GPU specifications.';
COMMENT ON COLUMN public.gpu.gpu_information
  IS 'The output of `nvidia-smi -q` (on Linux or Windows), or `cuda-smi` '
     'or `kextstat | grep -i cuda` on OSX, or another command; anything '
     'that gets a string to uniquely identify the GPU.';

-- CONSTRAINTS
CREATE INDEX gpu_index_on_part_number
  ON public.gpu (gpu_part_number);

CREATE INDEX gpu_index_on_product_name
  ON public.gpu (gpu_product_name);

CREATE INDEX gpu_index_on_product_name_and_part_number
  ON public.gpu (gpu_product_name, gpu_part_number);
