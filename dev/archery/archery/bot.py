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

import shlex
from functools import partial

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


class CommentBot:

    def __init__(self, name, handler, token=None):
        # TODO(kszucs): validate
        self.name = name
        self.handler = handler
        self.github = github.Github(token)

    def parse_command(self, payload):
        # only allow users of apache org to submit commands, for more see
        # https://developer.github.com/v4/enum/commentauthorassociation/
        allowed_roles = {'OWNER', 'MEMBER', 'CONTRIBUTOR'}
        mention = f'@{self.name}'
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
            print(e)
            pull.create_issue_comment("```\n{}\n```".format(e.message))
        except Exception as e:
            print(e)
            comment.create_reaction('-1')
        else:
            comment.create_reaction('+1')

    def handle_review_comment(self, payload):
        raise NotImplementedError()


command = partial(click.command, cls=Command)
group = partial(click.group, cls=Group)


@group(name='@ursabot')
@click.pass_context
def ursabot(ctx):
    """Ursabot"""
    ctx.ensure_object(dict)


@ursabot.group()
@click.option('--arrow', '-a', default='apache/arrow',
              help='Arrow repository on github to use')
@click.option('--crossbow', '-c', default='ursa-labs/crossbow',
              help='Crossbow repository on github to use')
@click.pass_obj
def crossbow(obj, arrow, crossbow):
    """Trigger crossbow builds for this pull request"""
    # obj['arrow_repo'] = 'https://github.com/{}'.format(arrow)
    obj['crossbow_repo'] = 'https://github.com/{}'.format(crossbow)


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
    args = []
    for g in group:
        args.extend(['-g', g])
    for t in task:
        args.append(t)

    git = Git()
    xbow = Crossbow('arrow/dev/tasks/crossbow.py')

    # arrow is already cloned ideally but of course we should be able to choose
    # a different fork
    # git.clone(obj['arrow_repo'], 'arrow')
    git.clone(obj['crossbow_repo'], 'crossbow')
    xbow.run('submit', *args)