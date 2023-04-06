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

import enum
import os
import shlex
from pathlib import Path
from functools import lru_cache, partial
import tempfile

import click
import github

from .utils.git import git
from .utils.logger import logger
from .crossbow import Repo, Queue, Config, Target, Job, CommentReport


def cached_property(fn):
    return property(lru_cache(maxsize=1)(fn))


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


LABEL_PREFIX = "awaiting"


@enum.unique
class PullRequestState(enum.Enum):
    """State of a pull request."""

    review = f"{LABEL_PREFIX} review"
    committer_review = f"{LABEL_PREFIX} committer review"
    changes = f"{LABEL_PREFIX} changes"
    change_review = f"{LABEL_PREFIX} change review"
    merge = f"{LABEL_PREFIX} merge"


COMMITTER_ROLES = {'OWNER', 'MEMBER'}


class PullRequestWorkflowBot:

    def __init__(self, event_name, event_payload, token=None, committers=None):
        self.github = github.Github(token)
        self.event_name = event_name
        self.event_payload = event_payload
        self.committers = committers

    @cached_property
    def pull(self):
        """
        Returns a github.PullRequest object associated with the event.
        """
        return self.repo.get_pull(self.event_payload['pull_request']['number'])

    @cached_property
    def repo(self):
        return self.github.get_repo(self.event_payload['repository']['id'], lazy=True)

    def is_committer(self, action):
        """
        Returns whether the author of the action is a committer or not.
        If the list of committer usernames is not available it will use the
        author_association as a fallback mechanism.
        """
        if self.committers:
            return (self.event_payload[action]['user']['login'] in
                    self.committers)
        return (self.event_payload[action]['author_association'] in
                COMMITTER_ROLES)

    def handle(self):
        current_state = None
        try:
            current_state = self.get_current_state()
        except EventError:
            # In case of error (more than one state) we clear state labels
            # only possible if a label has been manually added.
            self.clear_current_state()
        next_state = self.compute_next_state(current_state)
        if not current_state or current_state != next_state:
            if current_state:
                self.clear_current_state()
            self.set_state(next_state)

    def get_current_state(self):
        """
        Returns a PullRequestState with the current PR state label
        based on label starting with LABEL_PREFIX.
        If more than one label is found raises EventError.
        If no label is found returns None.
        """
        states = [label.name for label in self.pull.get_labels()
                  if label.name.startswith(LABEL_PREFIX)]
        if len(states) > 1:
            raise EventError(f"PR cannot be on more than one states - {states}")
        elif states:
            return PullRequestState(states[0])

    def clear_current_state(self):
        """
        Removes all existing labels starting with LABEL_PREFIX
        """
        for label in self.pull.get_labels():
            if label.name.startswith(LABEL_PREFIX):
                self.pull.remove_from_labels(label)

    def compute_next_state(self, current_state):
        """
        Returns the expected next state based on the event and
        the current state.
        """
        if (self.event_name == "pull_request_target" and
                self.event_payload['action'] == 'opened'):
            if self.is_committer('pull_request'):
                return PullRequestState.committer_review
            else:
                return PullRequestState.review
        elif (self.event_name == "pull_request_review" and
                self.event_payload["action"] == "submitted"):
            review_state = self.event_payload["review"]["state"].lower()
            if not self.is_committer('review'):
                # Non-committer reviews cannot change state once committer has already
                # reviewed, requested changes or approved
                if current_state in (
                        PullRequestState.change_review,
                        PullRequestState.changes,
                        PullRequestState.merge):
                    return current_state
                else:
                    return PullRequestState.committer_review
            if review_state == 'approved':
                return PullRequestState.merge
            else:
                return PullRequestState.changes
        elif (self.event_name == "pull_request_target" and
              self.event_payload['action'] == 'synchronize' and
              current_state == PullRequestState.changes):
            return PullRequestState.change_review
        # Default already opened PRs to Review state.
        if current_state is None:
            current_state = PullRequestState.review
        return current_state

    def set_state(self, state):
        """Sets the State label to the PR."""
        self.pull.add_to_labels(state.value)


class CommentBot:

    def __init__(self, name, handler, token=None):
        # TODO(kszucs): validate
        assert isinstance(name, str)
        assert callable(handler)
        self.name = name
        self.handler = handler
        self.github = github.Github(token)

    def parse_command(self, payload):
        mention = '@{}'.format(self.name)
        comment = payload['comment']

        if payload['sender']['login'] == self.name:
            raise EventError("Don't respond to itself")
        elif payload['action'] not in {'created', 'edited'}:
            raise EventError("Don't respond to comment deletion")
        elif not comment['body'].lstrip().startswith(mention):
            raise EventError("The bot is not mentioned")

        # Parse the comment, removing the bot mentioned (and everything
        # before it)
        command = payload['comment']['body'].split(mention)[-1]

        # then split on newlines and keep only the first line
        # (ignoring all other lines)
        return command.split("\n")[0].strip()

    def handle(self, event, payload):
        try:
            command = self.parse_command(payload)
        except EventError as e:
            logger.error(e)
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
            # Only allow users of apache org to submit commands, for more see
            # https://developer.github.com/v4/enum/commentauthorassociation/
            # Checking  privileges here enables the bot to respond
            # without relying on the handler.
            allowed_roles = {'OWNER', 'MEMBER', 'CONTRIBUTOR', 'COLLABORATOR'}
            if payload['comment']['author_association'] not in allowed_roles:
                raise EventError(
                    "Only contributors can submit requests to this bot. "
                    "Please ask someone from the community for help with "
                    "getting the first commit in."
                )
            self.handler(command, issue=issue, pull_request=pull,
                         comment=comment)
        except Exception as e:
            logger.exception(e)
            url = "{server}/{repo}/actions/runs/{run_id}".format(
                server=os.environ["GITHUB_SERVER_URL"],
                repo=os.environ["GITHUB_REPOSITORY"],
                run_id=os.environ["GITHUB_RUN_ID"],
            )
            pull.create_issue_comment(
                f"```\n{e}\nThe Archery job run can be found at: {url}\n```")
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
@click.option('--crossbow', '-c', default='ursacomputing/crossbow',
              help='Crossbow repository on github to use')
@click.pass_obj
def crossbow(obj, crossbow):
    """
    Trigger crossbow builds for this pull request
    """
    obj['crossbow_repo'] = crossbow


def _clone_arrow_and_crossbow(dest, crossbow_repo, pull_request):
    """
    Clone the repositories and initialize crossbow objects.

    Parameters
    ----------
    dest : Path
        Filesystem path to clone the repositories to.
    crossbow_repo : str
        Github repository name, like kszucs/crossbow.
    pull_request : pygithub.PullRequest
        Object containing information about the pull request the comment bot
        was triggered from.
    """
    arrow_path = dest / 'arrow'
    queue_path = dest / 'crossbow'

    # clone arrow and checkout the pull request's branch
    pull_request_ref = 'pull/{}/head:{}'.format(
        pull_request.number, pull_request.head.ref
    )
    git.clone(pull_request.base.repo.clone_url, str(arrow_path))
    git.fetch('origin', pull_request_ref, git_dir=arrow_path)
    git.checkout(pull_request.head.ref, git_dir=arrow_path)

    # clone crossbow repository
    crossbow_url = 'https://github.com/{}'.format(crossbow_repo)
    git.clone(crossbow_url, str(queue_path))

    # initialize crossbow objects
    github_token = os.environ['CROSSBOW_GITHUB_TOKEN']
    arrow = Repo(arrow_path)
    queue = Queue(queue_path, github_token=github_token, require_https=True)

    return (arrow, queue)


@crossbow.command()
@click.argument('tasks', nargs=-1, required=False)
@click.option('--group', '-g', 'groups', multiple=True,
              help='Submit task groups as defined in tests.yml')
@click.option('--param', '-p', 'params', multiple=True,
              help='Additional task parameters for rendering the CI templates')
@click.option('--arrow-version', '-v', default=None,
              help='Set target version explicitly.')
@click.option('--wait', default=60,
              help='Wait the specified seconds before generating a report.')
@click.pass_obj
def submit(obj, tasks, groups, params, arrow_version, wait):
    """
    Submit crossbow testing tasks.

    See groups defined in arrow/dev/tasks/tasks.yml
    """
    crossbow_repo = obj['crossbow_repo']
    pull_request = obj['pull_request']
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        arrow, queue = _clone_arrow_and_crossbow(
            dest=Path(tmpdir),
            crossbow_repo=crossbow_repo,
            pull_request=pull_request,
        )
        # load available tasks configuration and groups from yaml
        config = Config.load_yaml(arrow.path / "dev" / "tasks" / "tasks.yml")
        config.validate()

        # initialize the crossbow build's target repository
        target = Target.from_repo(arrow, version=arrow_version,
                                  remote=pull_request.head.repo.clone_url,
                                  branch=pull_request.head.ref)

        # parse additional job parameters
        params = dict([p.split("=") for p in params])
        params['pr_number'] = pull_request.number

        # instantiate the job object
        job = Job.from_config(config=config, target=target, tasks=tasks,
                              groups=groups, params=params)

        # add the job to the crossbow queue and push to the remote repository
        queue.put(job, prefix="actions", increment_job_id=False)
        queue.push()

        # render the response comment's content
        report = CommentReport(job, crossbow_repo=crossbow_repo,
                               wait_for_task=wait)

        # send the response
        pull_request.create_issue_comment(report.show())
