#!/usr/bin/python
##############################################################################
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
##############################################################################

# This script generates a series of shell commands
# to rebase all open pull requests off of master
# and force push the updates.

from http.client import HTTPSConnection
import json
from collections import defaultdict

client = HTTPSConnection('api.github.com')
client.request('GET',
               '/repos/apache/arrow/pulls?state=open&per_page=100',
               headers={'User-Agent': 'ApacheArrowRebaser'})
response = client.getresponse()
json_content = response.read()
if response.status != 200:
    error_msg = 'GitHub connection error:{}'.format(json_content)
    raise Exception(error_msg)

parsed_content = json.loads(json_content)
if len(parsed_content) == 100:
    print("# WARNING: Only the most recent 100 PRs will be processed")

repos = defaultdict(list)
for pr in parsed_content:
    head = pr['head']
    repos[head['repo']['full_name']].append(head['label'])

for repo, labels in repos.items():
    print('git clone git@github.com:{}.git'.format(repo))
    print('cd arrow')
    print('git remote add upstream https://github.com/apache/arrow.git')
    print('git fetch --all --prune --tags --force')
    for label in labels:
        # Labels are in the form 'user:branch'
        owner, branch = label.split(':')
        print('git checkout {}'.format(branch))
        print('(git rebase upstream/master && git push --force) || ' +
              '(echo "Rebase failed for {}" && '.format(label) +
              'git rebase --abort)')
    print('cd ..')
    print('rm -rf arrow')
