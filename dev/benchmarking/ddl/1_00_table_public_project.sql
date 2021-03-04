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


-- PROJECT
CREATE TABLE IF NOT EXISTS public.project
(
  project_id SERIAL PRIMARY KEY
  , project_name citext NOT NULL
  , project_url text NOT NULL
  , repo_url text NOT NULL
  , last_changed timestamp (0) without time zone NOT NULL DEFAULT now()
);
COMMENT ON TABLE public.project
  IS 'Project name and relevant URLs.';
COMMENT ON COLUMN public.project.project_url
  IS 'Homepage URL.';
COMMENT ON COLUMN public.project.repo_url
  IS 'Git repo URL to link stored commit hashes to code in a webpage.';
COMMENT ON COLUMN public.project.last_changed
  IS 'New project details are added with a new timestamp. '
     'The project details with the newest timestamp will be used.';

-- CONSTRAINTS
CREATE UNIQUE INDEX project_unique_index_on_project_name_urls
  ON public.project(project_name, project_url, repo_url);
COMMENT ON INDEX
  public.project_unique_index_on_project_name_urls
  IS 'Enforce uniqueness of project name and urls.';
