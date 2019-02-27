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


-- LANGUAGE_IMPLEMENTATION_VERSION_VIEW_TRIGGER_INSERT
CREATE TRIGGER language_implementation_version_view_trigger_insert
  INSTEAD OF INSERT ON public.language_implementation_version_view
  FOR EACH ROW
  EXECUTE FUNCTION public.language_implementation_version_view_insert_row();

-- ENVIRONMENT_VIEW_TRIGGER_INSERT
CREATE TRIGGER environment_view_trigger_insert
  INSTEAD OF INSERT ON public.environment_view
  FOR EACH ROW
  EXECUTE FUNCTION public.environment_view_insert_row();

-- MACHINE_VIEW_TRIGGER_INSERT
CREATE TRIGGER machine_view_trigger_insert
  INSTEAD OF INSERT ON public.machine_view
  FOR EACH ROW
  EXECUTE FUNCTION public.machine_view_insert_row();

-- UNIT_VIEW_TRIGGER_INSERT
CREATE TRIGGER unit_view_trigger_insert
  INSTEAD OF INSERT ON public.unit_view
  FOR EACH ROW
  EXECUTE FUNCTION public.unit_view_insert_row();

-- BENCHMARK_VIEW_TRIGGER_INSERT
CREATE TRIGGER benchmark_view_trigger_insert
  INSTEAD OF INSERT ON public.benchmark_view
  FOR EACH ROW
  EXECUTE FUNCTION public.benchmark_view_insert_row();

-- BENCHMARK_RUN_VIEW_TRIGGER_INSERT
CREATE TRIGGER benchmark_run_view_trigger_insert
  INSTEAD OF INSERT ON public.benchmark_run_view
  FOR EACH ROW
  EXECUTE FUNCTION public.benchmark_run_view_insert_row();

-- FULL_BENCHMARK_RUN_VIEW_TRIGGER_INSERT
CREATE TRIGGER full_benchmark_run_view_trigger_insert
  INSTEAD OF INSERT ON public.full_benchmark_run_view
  FOR EACH ROW
  EXECUTE FUNCTION public.full_benchmark_run_view_insert_row();
