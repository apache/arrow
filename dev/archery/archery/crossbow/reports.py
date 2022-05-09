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

import click
import collections
import operator
import fnmatch
import functools

import requests

from archery.utils.report import JinjaReport


# TODO(kszucs): use archery.report.JinjaReport instead
class Report:

    def __init__(self, job, task_filters=None):
        self.job = job

        tasks = sorted(job.tasks.items())
        if task_filters:
            filtered = set()
            for pattern in task_filters:
                filtered |= set(fnmatch.filter(job.tasks.keys(), pattern))

            tasks = [(name, task) for name, task in tasks if name in filtered]

        self._tasks = dict(tasks)

    @property
    def repo_url(self):
        url = self.job.queue.remote_url
        return url[:-4] if url.endswith('.git') else url

    def url(self, query):
        return '{}/branches/all?query={}'.format(self.repo_url, query)

    def branch_url(self, branch):
        return '{}/tree/{}'.format(self.repo_url, branch)

    def task_url(self, task):
        if task.status().build_links:
            # show link to the actual build, some CI providers implement
            # the statuses API others implement the checks API, retrieve any.
            return task.status().build_links[0]
        else:
            # show link to the branch if no status build link was found.
            return self.branch_url(task.branch)

    @property
    @functools.lru_cache(maxsize=1)
    def tasks_by_state(self):
        tasks_by_state = collections.defaultdict(dict)
        for task_name, task in self.job.tasks.items():
            state = task.status().combined_state
            tasks_by_state[state][task_name] = task
        return tasks_by_state

    @property
    def tasks(self):
        return self._tasks

    def show(self):
        raise NotImplementedError()


class ConsoleReport(Report):
    """Report the status of a Job to the console using click"""

    # output table's header template
    HEADER = '[{state:>7}] {branch:<52} {content:>16}'
    DETAILS = ' └ {url}'

    # output table's row template for assets
    ARTIFACT_NAME = '{artifact:>69} '
    ARTIFACT_STATE = '[{state:>7}]'

    # state color mapping to highlight console output
    COLORS = {
        # from CombinedStatus
        'error': 'red',
        'failure': 'red',
        'pending': 'yellow',
        'success': 'green',
        # custom state messages
        'ok': 'green',
        'missing': 'red'
    }

    def lead(self, state, branch, n_uploaded, n_expected):
        line = self.HEADER.format(
            state=state.upper(),
            branch=branch,
            content='uploaded {} / {}'.format(n_uploaded, n_expected)
        )
        return click.style(line, fg=self.COLORS[state.lower()])

    def header(self):
        header = self.HEADER.format(
            state='state',
            branch='Task / Branch',
            content='Artifacts'
        )
        delimiter = '-' * len(header)
        return '{}\n{}'.format(header, delimiter)

    def artifact(self, state, pattern, asset):
        if asset is None:
            artifact = pattern
            state = 'pending' if state == 'pending' else 'missing'
        else:
            artifact = asset.name
            state = 'ok'

        name_ = self.ARTIFACT_NAME.format(artifact=artifact)
        state_ = click.style(
            self.ARTIFACT_STATE.format(state=state.upper()),
            self.COLORS[state]
        )
        return name_ + state_

    def show(self, outstream, asset_callback=None, validate_patterns=True):
        echo = functools.partial(click.echo, file=outstream)

        # write table's header
        echo(self.header())

        # write table's body
        for task_name, task in self.tasks.items():
            # write summary of the uploaded vs total assets
            status = task.status()
            assets = task.assets(validate_patterns=validate_patterns)

            # mapping of artifact pattern to asset or None of not uploaded
            n_expected = len(task.artifacts)
            n_uploaded = len(assets.uploaded_assets())
            echo(self.lead(status.combined_state, task_name, n_uploaded,
                           n_expected))

            # show link to the actual build, some of the CI providers implement
            # the statuses API others implement the checks API, so display both
            for link in status.build_links:
                echo(self.DETAILS.format(url=link))

            # write per asset status
            for artifact_pattern, asset in assets.items():
                if asset_callback is not None:
                    asset_callback(task_name, task, asset)
                echo(self.artifact(status.combined_state, artifact_pattern,
                                   asset))


class ChatReport(JinjaReport):
    templates = {
        'text': 'chat_nightly_report.txt.j2',
    }
    fields = [
        'report',
        'extra_message',
    ]


class ReportUtils:

    @classmethod
    def send_message(cls, webhook, message):
        resp = requests.post(webhook, json={
            "blocks": [
                {
                    "type": "section",
                    "text": {
                            "type": "mrkdwn",
                            "text": message
                    }
                }
            ]
        }
        )
        return resp

    @classmethod
    def send_email(cls, smtp_user, smtp_password, smtp_server, smtp_port,
                   recipient_email, message):
        import smtplib

        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.ehlo()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient_email, message)
        server.close()


class EmailReport(JinjaReport):
    templates = {
        'text': 'email_nightly_report.txt.j2',
    }
    fields = [
        'report',
        'sender_name',
        'sender_email',
        'recipient_email',
    ]


class CommentReport(Report):

    _markdown_badge = '[![{title}]({badge})]({url})'

    badges = {
        'github': _markdown_badge.format(
            title='Github Actions',
            url='https://github.com/{repo}/actions?query=branch:{branch}',
            badge=(
                'https://github.com/{repo}/workflows/Crossbow/'
                'badge.svg?branch={branch}'
            ),
        ),
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
            url='https://app.travis-ci.com/github/{repo}/branches',
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
        ),
        'drone': _markdown_badge.format(
            title='Drone',
            url='https://cloud.drone.io/{repo}',
            badge='https://img.shields.io/drone/build/{repo}/{branch}.svg'
        ),
    }

    def __init__(self, job, crossbow_repo):
        self.crossbow_repo = crossbow_repo
        super().__init__(job)

    def show(self):
        url = 'https://github.com/{repo}/branches/all?query={branch}'
        sha = self.job.target.head

        msg = 'Revision: {}\n\n'.format(sha)
        msg += 'Submitted crossbow builds: [{repo} @ {branch}]'
        msg += '({})\n'.format(url)
        msg += '\n|Task|Status|\n|----|------|'

        tasks = sorted(self.job.tasks.items(), key=operator.itemgetter(0))
        for key, task in tasks:
            branch = task.branch

            try:
                template = self.badges[task.ci]
                badge = template.format(
                    repo=self.crossbow_repo,
                    repo_dotted=self.crossbow_repo.replace('/', '.'),
                    branch=branch
                )
            except KeyError:
                badge = 'unsupported CI service `{}`'.format(task.ci)

            msg += '\n|{}|{}|'.format(key, badge)

        return msg.format(repo=self.crossbow_repo, branch=self.job.branch)
