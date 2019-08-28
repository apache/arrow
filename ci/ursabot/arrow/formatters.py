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

import json
import operator

import toolz
from tabulate import tabulate
from ruamel.yaml import YAML
from buildbot.util.logger import Logger
from buildbot.process.results import SUCCESS
from buildbot.process.properties import Properties

from ursabot.formatters import MarkdownFormatter


log = Logger()


class BenchmarkCommentFormatter(MarkdownFormatter):

    def _render_table(self, jsonlines):
        """Renders the json content of a result log

        As a plaintext table embedded in a diff markdown snippet.
        """
        rows = [json.loads(line.strip()) for line in jsonlines if line]

        columns = ['benchmark', 'baseline', 'contender', 'change']
        formatted = tabulate(toolz.pluck(columns, rows),
                             headers=columns, tablefmt='rst')

        diff = ['-' if row['regression'] else ' ' for row in rows]
        # prepend and append because of header and footer
        diff = [' '] * 3 + diff + [' ']

        rows = map(' '.join, zip(diff, formatted.splitlines()))
        table = '\n'.join(rows)

        return f'```diff\n{table}\n```'

    async def render_success(self, build, master):
        # extract logs named as `result`
        results = {}
        for step, log_lines in self.extract_logs(build, logname='result'):
            if step['results'] == SUCCESS:
                results[step['stepid']] = (line for _, line in log_lines)

        try:
            # decode jsonlines objects and render the results as markdown table
            # each step can have a result log, but in practice each builder
            # should use a single step for logging results, for more see
            # ursabot.steps.ResultLogMixin and usage at
            # ursabot.builders.ArrowCppBenchmark
            tables = toolz.valmap(self._render_table, results)
        except Exception as e:
            # TODO(kszucs): nicer message
            log.error(e)
            raise

        context = '\n\n'.join(tables.values())
        return dict(status='has been succeeded', context=context)


class CrossbowCommentFormatter(MarkdownFormatter):

    _markdown_badge = '[![{title}]({badge})]({url})'

    badges = {
        'azure': _markdown_badge.format(
            title='Azure',
            url=(
                'https://dev.azure.com/{repo}/_build/latest'
                '?definitionId=1&branchName={branch}'
            ),
            badge=(
                'https://dev.azure.com/{repo}/_apis/build/status/'
                '{repo_dotted}?branchName={branch}'
            )
        ),
        'travis': _markdown_badge.format(
            title='TravisCI',
            url='https://travis-ci.org/{repo}/branches',
            badge='https://img.shields.io/travis/{repo}/{branch}.svg'
        ),
        'circle': _markdown_badge.format(
            title='CircleCI',
            url='https://circleci.com/gh/{repo}/tree/{branch}',
            badge=(
                'https://img.shields.io/circleci/build/github'
                '/{repo}/{branch}.svg'
            )
        ),
        'appveyor': _markdown_badge.format(
            title='Appveyor',
            url='https://ci.appveyor.com/project/{repo}/history',
            badge='https://img.shields.io/appveyor/ci/{repo}/{branch}.svg'
        )
    }

    def __init__(self, *args, crossbow_repo, **kwargs):
        self.crossbow_repo = crossbow_repo
        self.yaml_parser = YAML()
        super().__init__(*args, **kwargs)

    def _render_message(self, yaml_lines, crossbow_repo):
        yaml_content = '\n'.join(yaml_lines)
        job = self.yaml_parser.load(yaml_content)

        url = 'https://github.com/{repo}/branches/all?query={branch}'
        msg = f'Submitted crossbow builds: [{{repo}} @ {{branch}}]({url})\n'
        msg += '\n|Task|Status|\n|----|------|'

        tasks = sorted(job['tasks'].items(), key=operator.itemgetter(0))
        for key, task in tasks:
            branch = task['branch']

            try:
                template = self.badges[task['ci']]
                badge = template.format(
                    repo=crossbow_repo,
                    repo_dotted=crossbow_repo.replace('/', '.'),
                    branch=branch
                )
            except KeyError:
                badge = 'unsupported CI service `{}`'.format(task['ci'])

            msg += f'\n|{key}|{badge}|'

        return msg.format(repo=crossbow_repo, branch=job['branch'])

    async def render_success(self, build, master):
        # extract logs named as `result`
        results = {}
        for step, log_lines in self.extract_logs(build, logname='result'):
            if step['results'] == SUCCESS:
                results[step['stepid']] = (line for _, line in log_lines)

        # render the crossbow repo, becuase it might be passed as a Property
        props = Properties.fromDict(build['properties'])
        props.master = master
        crossbow_repo = await props.render(self.crossbow_repo)
        if not isinstance(crossbow_repo, str):
            raise ValueError('crossbow_repo argument must be a string')

        try:
            # decode yaml objects and render the results as a github links
            # pointing to the pushed crossbow branches
            messages = [
                self._render_message(yaml_lines, crossbow_repo=crossbow_repo)
                for yaml_lines in results.values()
            ]
        except Exception as e:
            log.error(e)
            raise

        context = '\n\n'.join(messages)
        return dict(status='has been succeeded', context=context)
