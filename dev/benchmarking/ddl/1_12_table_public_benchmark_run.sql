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


-- BENCHMARK_RUN
CREATE TABLE IF NOT EXISTS public.benchmark_run
(
  benchmark_run_id BIGSERIAL PRIMARY KEY
  , parameter_values jsonb NOT NULL DEFAULT '{}'::jsonb
  , value numeric NOT NULL
  , git_commit_timestamp timestamp (0) with time zone NOT NULL
  , git_hash text NOT NULL
  , val_min numeric
  , val_q1 numeric
  , val_q3 numeric
  , val_max numeric
  , std_dev numeric NOT NULL
  , n_obs integer NOT NULL
  , run_timestamp timestamp (0) with time zone NOT NULL
  , run_metadata jsonb
  , run_notes text
  , machine_id integer NOT NULL
  , environment_id integer NOT NULL
  , language_implementation_version_id integer NOT NULL
  , benchmark_language_id integer NOT NULL
  , benchmark_id integer NOT NULL
  , FOREIGN KEY (machine_id) REFERENCES public.machine
  , FOREIGN KEY
      (environment_id, benchmark_language_id, language_implementation_version_id)
      REFERENCES public.environment
  , FOREIGN KEY (benchmark_id, benchmark_language_id)
      REFERENCES public.benchmark(benchmark_id, benchmark_language_id)
);
COMMENT ON TABLE public.benchmark_run
  IS 'One run per benchmark run.';
COMMENT ON COLUMN public.benchmark_run.parameter_values
  IS 'A JSON object mapping the parameter names from '
     '"benchmark.parameter_names" to values.';
COMMENT ON COLUMN public.benchmark_run.value
  IS 'The average value from the benchmark run.';
COMMENT ON COLUMN public.benchmark_run.git_commit_timestamp
  IS 'Get this using `git show -s --date=local --format="%ci" <hash>`. '
     'ISO 8601 is recommended, e.g. ''2019-01-30 03:12 -0600''.';
COMMENT ON COLUMN public.benchmark_run.git_hash
  IS 'The commit has of the codebase currently being benchmarked.';
COMMENT ON COLUMN public.benchmark_run.val_min
  IS 'The smallest benchmark run value for this run.';
COMMENT ON COLUMN public.benchmark_run.val_q1
  IS 'The first quartile of the benchmark run values for this run.';
COMMENT ON COLUMN public.benchmark_run.val_q3
  IS 'The third quartile of the benchmark run values for this run.';
COMMENT ON COLUMN public.benchmark_run.val_max
  IS 'The largest benchmark run value for this run.';
COMMENT ON COLUMN public.benchmark_run.std_dev
  IS 'The standard deviation of the run values for this benchmark run.';
COMMENT ON COLUMN public.benchmark_run.n_obs
  IS 'The number of observations for this benchmark run.';
COMMENT ON COLUMN public.benchmark_run.run_metadata
  IS 'Additional metadata of interest, as a JSON object. '
     'For example: ''{"ci_99": [2.7e-06, 3.1e-06]}''::jsonb.';
COMMENT ON COLUMN public.benchmark_run.run_notes
  IS 'Additional notes of interest, as a text string. ';

-- CONSTRAINTS
ALTER TABLE public.benchmark_run
  ADD CONSTRAINT benchmark_run_check_std_dev_nonnegative
  CHECK (std_dev >= 0);

ALTER TABLE public.benchmark_run
  ADD CONSTRAINT benchmark_run_check_n_obs_positive
  CHECK (n_obs > 0);

CREATE INDEX benchmark_run_index_on_environment_id
  ON public.benchmark_run(environment_id);

CREATE INDEX benchmark_run_index_on_machine_id
  ON public.benchmark_run(machine_id);

CREATE INDEX benchmark_run_index_on_benchmark_id
  ON public.benchmark_run(benchmark_id, benchmark_language_id);

CREATE INDEX benchmark_run_index_on_benchmark_environment_time
  ON public.benchmark_run
    (benchmark_id, environment_id, git_commit_timestamp);
COMMENT ON INDEX
  public.benchmark_run_index_on_benchmark_environment_time
  IS 'Index to improve sorting by benchmark, environment, and timestamp.';

CREATE UNIQUE INDEX
  benchmark_run_unique_index_on_env_benchmark_timestamp_params
  ON public.benchmark_run
    (machine_id, environment_id, benchmark_id, git_commit_timestamp, parameter_values, run_timestamp);
COMMENT ON INDEX
  public.benchmark_run_unique_index_on_env_benchmark_timestamp_params
  IS 'Enforce uniqueness of benchmark run for a given machine, '
     'environment, benchmark, git commit timestamp, and parameter values.';
