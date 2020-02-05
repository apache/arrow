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


-- MACHINE
CREATE TABLE IF NOT EXISTS public.machine
(
  machine_id SERIAL PRIMARY KEY
  , machine_name citext NOT NULL
  , mac_address macaddr NOT NULL
  , memory_bytes bigint NOT NULL
  , cpu_actual_frequency_Hz bigint NOT NULL
  , machine_other_attributes jsonb
  , cpu_id integer NOT NULL
  , gpu_id integer NOT NULL
  , os_id integer NOT NULL
  , FOREIGN KEY (cpu_id) REFERENCES public.cpu
  , FOREIGN KEY (gpu_id) REFERENCES public.gpu
  , FOREIGN KEY (os_id) REFERENCES public.os
);
COMMENT ON TABLE public.machine
  IS 'Unique identifiers for a machine.';
COMMENT ON COLUMN public.machine.machine_name
  IS 'A machine name of your choice.';
COMMENT ON COLUMN public.machine.mac_address
  IS 'The mac_address of a physical network interface to uniquely '
     'identify a computer. Postgres accepts standard formats, including '
     '''08:00:2b:01:02:03'', ''08-00-2b-01-02-03'', ''08002b:010203''';
COMMENT ON COLUMN public.machine.memory_bytes
  IS 'The output of `sysctl -n hw.memsize`.';
COMMENT ON COLUMN public.machine.cpu_actual_frequency_Hz
  IS 'The output of `sysctl -n hw.cpufrequency`.';
COMMENT ON COLUMN public.machine.machine_other_attributes
  IS 'Additional attributes of interest, as a JSON object. '
     'For example: ''{"hard_disk_type": "solid state"}''::jsonb.';

-- CONSTRAINTS
CREATE UNIQUE INDEX machine_index_on_mac_address
  ON public.machine(mac_address);
COMMENT ON INDEX machine_index_on_mac_address
  IS 'Enforce unique mac address';

CREATE INDEX machine_index_on_cpu_id
  ON public.machine(cpu_id);

CREATE INDEX machine_index_on_gpu_id
  ON public.machine(gpu_id);

CREATE INDEX machine_index_on_os_id
  ON public.machine(os_id);

CREATE INDEX machine_index_on_cpu_gpu_os_id
  ON public.machine(cpu_id, gpu_id, os_id);
