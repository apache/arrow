# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from functools import cached_property

import requests


class Workflow:
    def __init__(self, workflow_id, repository, ignore_job, gh_token=None):
        self.workflow_id = workflow_id
        self.gh_token = gh_token
        self.repository = repository
        self.ignore_job = ignore_job
        self.headers = {
            'Accept': 'application/vnd.github.v3+json',
        }
        if self.gh_token:
            self.headers["Authorization"] = f"Bearer {self.gh_token}"
        workflow_resp = requests.get(
            f'https://api.github.com/repos/{repository}/actions/runs/{workflow_id}',
            headers=self.headers
        )
        if workflow_resp.status_code == 200:
            self.workflow_data = workflow_resp.json()

        else:
            # TODO: We could send an error report instead
            raise Exception(
                f'Failed to fetch workflow data: {workflow_resp.status_code}')

    @property
    def conclusion(self):
        return self.workflow_data.get('conclusion')

    @property
    def jobs_url(self):
        return self.workflow_data.get('jobs_url')

    @property
    def name(self):
        return self.workflow_data.get('name')

    @property
    def url(self):
        return self.workflow_data.get('html_url')

    @cached_property
    def jobs(self):
        jobs = []
        jobs_resp = requests.get(self.jobs_url, headers=self.headers)
        if jobs_resp.status_code == 200:
            jobs_data = jobs_resp.json()
            for job_data in jobs_data.get('jobs', []):
                if job_data.get('name') != self.ignore_job:
                    job = Job(job_data)
                    jobs.append(job)
        return jobs

    def failed_jobs(self):
        return [job for job in self.jobs if not job.is_successful()]

    def successful_jobs(self):
        return [job for job in self.jobs if job.is_successful()]


class Job:
    def __init__(self, job_data):
        self.job_data = job_data

    @property
    def conclusion(self):
        return self.job_data.get('conclusion')

    @property
    def name(self):
        return self.job_data.get('name')

    @property
    def url(self):
        return self.job_data.get('html_url')

    def is_successful(self):
        return self.conclusion == 'success'
