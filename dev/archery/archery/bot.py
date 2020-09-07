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

import operator
import shlex
from pathlib import Path
from functools import partial
import tempfile

import click
import github

from .utils.crossbow import Crossbow
from .utils.git import Git


class EventError(Exception):
    pass


class CommandError(Exception):

    def __init__(self, message):
        self.message = message


class _CommandMixin:

    def get_help_option(self, ctx):
        def show_help(ctx, param, value):
            if value and not ctx.resilient_parsing:
                raise click.UsageError(ctx.get_help())
        option = super().get_help_option(ctx)
        option.callback = show_help
        return option

    def __call__(self, message, **kwargs):
        args = shlex.split(message)
        try:
            with self.make_context(self.name, args=args, obj=kwargs) as ctx:
                return self.invoke(ctx)
        except click.ClickException as e:
            raise CommandError(e.format_message())


class Command(_CommandMixin, click.Command):
    pass


class Group(_CommandMixin, click.Group):

    def command(self, *args, **kwargs):
        kwargs.setdefault('cls', Command)
        return super().command(*args, **kwargs)

    def group(self, *args, **kwargs):
        kwargs.setdefault('cls', Group)
        return super().group(*args, **kwargs)

    def parse_args(self, ctx, args):
        if not args and self.no_args_is_help and not ctx.resilient_parsing:
            raise click.UsageError(ctx.get_help())
        return super().parse_args(ctx, args)


command = partial(click.command, cls=Command)
group = partial(click.group, cls=Group)


class CrossbowCommentFormatter:

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
        ),
        'drone': _markdown_badge.format(
            title='Drone',
            url='https://cloud.drone.io/{repo}',
            badge='https://img.shields.io/drone/build/{repo}/{branch}.svg'
        ),
    }

    def __init__(self, crossbow_repo):
        self.crossbow_repo = crossbow_repo

    def render(self, job):
        url = 'https://github.com/{repo}/branches/all?query={branch}'
        sha = job['target']['head']

        msg = 'Revision: {}\n\n'.format(sha)
        msg += 'Submitted crossbow builds: [{repo} @ {branch}]'
        msg += '({})\n'.format(url)
        msg += '\n|Task|Status|\n|----|------|'

        tasks = sorted(job['tasks'].items(), key=operator.itemgetter(0))
        for key, task in tasks:
            branch = task['branch']

            try:
                template = self.badges[task['ci']]
                badge = template.format(
                    repo=self.crossbow_repo,
                    repo_dotted=self.crossbow_repo.replace('/', '.'),
                    branch=branch
                )
            except KeyError:
                badge = 'unsupported CI service `{}`'.format(task['ci'])

            msg += '\n|{}|{}|'.format(key, badge)

        return msg.format(repo=self.crossbow_repo, branch=job['branch'])


class CommentBot:

    def __init__(self, name, handler, token=None):
        # TODO(kszucs): validate
        assert isinstance(name, str)
        assert callable(handler)
        self.name = name
        self.handler = handler
        self.github = github.Github(token)

    def parse_command(self, payload):
        # only allow users of apache org to submit commands, for more see
        # https://developer.github.com/v4/enum/commentauthorassociation/
        allowed_roles = {'OWNER', 'MEMBER', 'CONTRIBUTOR'}
        mention = '@{}'.format(self.name)
        comment = payload['comment']

        if payload['sender']['login'] == self.name:
            raise EventError("Don't respond to itself")
        elif payload['action'] not in {'created', 'edited'}:
            raise EventError("Don't respond to comment deletion")
        elif comment['author_association'] not in allowed_roles:
            raise EventError(
                "Don't respond to comments from non-authorized users"
            )
        elif not comment['body'].lstrip().startswith(mention):
            raise EventError("The bot is not mentioned")

        return payload['comment']['body'].split(mention)[-1].strip()

    def handle(self, event, payload):
        try:
            command = self.parse_command(payload)
        except EventError as e:
            print(e)
            # TODO(kszucs): log
            # see the possible reasons in the validate method
            return

        if event == 'issue_comment':
            return self.handle_issue_comment(command, payload)
        elif event == 'pull_request_review_comment':
            return self.handle_review_comment(command, payload)
        else:
            raise ValueError("Unexpected event type {}".format(event))

    def handle_issue_comment(self, command, payload):
        repo = self.github.get_repo(payload['repository']['id'], lazy=True)
        issue = repo.get_issue(payload['issue']['number'])

        try:
            pull = issue.as_pull_request()
        except github.GithubException:
            return issue.create_comment(
                "The comment bot only listens to pull request comments!"
            )

        comment = pull.get_issue_comment(payload['comment']['id'])
        try:
            self.handler(command, issue=issue, pull=pull, comment=comment)
        except CommandError as e:
            # TODO(kszucs): log
            print(e)
            pull.create_issue_comment("```\n{}\n```".format(e.message))
        except Exception as e:
            # TODO(kszucs): log
            print(e)
            comment.create_reaction('-1')
        else:
            comment.create_reaction('+1')

    def handle_review_comment(self, payload):
        raise NotImplementedError()


@group(name='@github-actions')
@click.pass_context
def actions(ctx):
    """Ursabot"""
    ctx.ensure_object(dict)


@actions.group()
@click.option('--crossbow', '-c', default='ursa-labs/crossbow',
              help='Crossbow repository on github to use')
@click.pass_obj
def crossbow(obj, crossbow):
    """Trigger crossbow builds for this pull request"""
    obj['crossbow_repo'] = crossbow


@crossbow.command()
@click.argument('task', nargs=-1, required=False)
@click.option('--group', '-g', multiple=True,
              help='Submit task groups as defined in tests.yml')
@click.option('--dry-run/--push', default=False,
              help='Just display the new changelog, don\'t write it')
@click.pass_obj
def submit(obj, task, group, dry_run):
    """Submit crossbow testing tasks.

    See groups defined in arrow/dev/tasks/tests.yml
    """
    from ruamel.yaml import YAML

    git = Git()

    # construct crossbow arguments
    args = []
    if dry_run:
        args.append('--dry-run')

    for g in group:
        args.extend(['-g', g])
    for t in task:
        args.append(t)

    # pygithub pull request object
    pr = obj['pull']
    crossbow_url = 'https://github.com/{}'.format(obj['crossbow_repo'])

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        arrow = tmpdir / 'arrow'
        queue = tmpdir / 'crossbow'

        # clone arrow and checkout the pull request's branch
        git.clone(pr.base.repo.clone_url, str(arrow))
        git.fetch('origin', 'pull/{}/head:{}'.format(pr.number, pr.head.ref),
                  git_dir=str(arrow))
        git.checkout(pr.head.ref, git_dir=str(arrow))

        # clone crossbow
        git.clone(crossbow_url, str(queue))

        # submit the crossbow tasks
        result = Path('result.yml').resolve()
        xbow = Crossbow(str(arrow / 'dev' / 'tasks' / 'crossbow.py'))
        xbow.run(
            '--queue-path', str(queue),
            '--output-file', str(result),
            'submit',
            '--job-prefix', 'actions',
            # don't rely on crossbow's remote and branch detection, because
            # it doesn't work without a tracking upstream branch
            '--arrow-remote', pr.head.repo.clone_url,
            '--arrow-branch', pr.head.ref,
            *args
        )

    # parse the result yml describing the submitted job
    yaml = YAML()
    with result.open() as fp:
        job = yaml.load(fp)

    # render the response comment's content
    formatter = CrossbowCommentFormatter(obj['crossbow_repo'])
    response = formatter.render(job)

    # send the response
    pr.create_issue_comment(response)
