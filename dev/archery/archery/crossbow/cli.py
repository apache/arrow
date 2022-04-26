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

from pathlib import Path
import time

import click

from .core import Config, Repo, Queue, Target, Job, CrossbowError
from .reports import EmailReport, ConsoleReport
from ..utils.source import ArrowSources


_default_arrow_path = ArrowSources.find().path
_default_queue_path = _default_arrow_path.parent / "crossbow"
_default_config_path = _default_arrow_path / "dev" / "tasks" / "tasks.yml"


@click.group()
@click.option('--github-token', '-t', default=None,
              envvar="CROSSBOW_GITHUB_TOKEN",
              help='OAuth token for GitHub authentication')
@click.option('--arrow-path', '-a',
              type=click.Path(), default=_default_arrow_path,
              help='Arrow\'s repository path. Defaults to the repository of '
                   'this script')
@click.option('--queue-path', '-q',
              envvar="CROSSBOW_QUEUE_PATH",
              type=click.Path(), default=_default_queue_path,
              help='The repository path used for scheduling the tasks. '
                   'Defaults to crossbow directory placed next to arrow')
@click.option('--queue-remote', '-qr', default=None,
              help='Force to use this remote URL for the Queue repository')
@click.option('--output-file', metavar='<output>',
              type=click.File('w', encoding='utf8'), default='-',
              help='Capture output result into file.')
@click.pass_context
def crossbow(ctx, github_token, arrow_path, queue_path, queue_remote,
             output_file):
    """
    Schedule packaging tasks or nightly builds on CI services.
    """
    ctx.ensure_object(dict)
    ctx.obj['output'] = output_file
    ctx.obj['arrow'] = Repo(arrow_path)
    ctx.obj['queue'] = Queue(queue_path, remote_url=queue_remote,
                             github_token=github_token, require_https=True)


@crossbow.command()
@click.option('--config-path', '-c',
              type=click.Path(exists=True), default=_default_config_path,
              help='Task configuration yml. Defaults to tasks.yml')
@click.pass_obj
def check_config(obj, config_path):
    # load available tasks configuration and groups from yaml
    config = Config.load_yaml(config_path)
    config.validate()

    output = obj['output']
    config.show(output)


@crossbow.command()
@click.argument('tasks', nargs=-1, required=False)
@click.option('--group', '-g', 'groups', multiple=True,
              help='Submit task groups as defined in task.yml')
@click.option('--param', '-p', 'params', multiple=True,
              help='Additional task parameters for rendering the CI templates')
@click.option('--job-prefix', default='build',
              help='Arbitrary prefix for branch names, e.g. nightly')
@click.option('--config-path', '-c',
              type=click.Path(exists=True), default=_default_config_path,
              help='Task configuration yml. Defaults to tasks.yml')
@click.option('--arrow-version', '-v', default=None,
              help='Set target version explicitly.')
@click.option('--arrow-remote', '-r', default=None,
              help='Set GitHub remote explicitly, which is going to be cloned '
                   'on the CI services. Note, that no validation happens '
                   'locally. Examples: https://github.com/apache/arrow or '
                   'https://github.com/kszucs/arrow.')
@click.option('--arrow-branch', '-b', default=None,
              help='Give the branch name explicitly, e.g. master, ARROW-1949.')
@click.option('--arrow-sha', '-t', default=None,
              help='Set commit SHA or Tag name explicitly, e.g. f67a515, '
                   'apache-arrow-0.11.1.')
@click.option('--fetch/--no-fetch', default=True,
              help='Fetch references (branches and tags) from the remote')
@click.option('--dry-run/--commit', default=False,
              help='Just display the rendered CI configurations without '
                   'committing them')
@click.option('--no-push/--push', default=False,
              help='Don\'t push the changes')
@click.pass_obj
def submit(obj, tasks, groups, params, job_prefix, config_path, arrow_version,
           arrow_remote, arrow_branch, arrow_sha, fetch, dry_run, no_push):
    output = obj['output']
    queue, arrow = obj['queue'], obj['arrow']

    # load available tasks configuration and groups from yaml
    config = Config.load_yaml(config_path)
    try:
        config.validate()
    except CrossbowError as e:
        raise click.ClickException(str(e))

    # Override the detected repo url / remote, branch and sha - this aims to
    # make release procedure a bit simpler.
    # Note, that the target resivion's crossbow templates must be
    # compatible with the locally checked out version of crossbow (which is
    # in case of the release procedure), because the templates still
    # contain some business logic (dependency installation, deployments)
    # which will be reduced to a single command in the future.
    target = Target.from_repo(arrow, remote=arrow_remote, branch=arrow_branch,
                              head=arrow_sha, version=arrow_version)

    # parse additional job parameters
    params = dict([p.split("=") for p in params])

    # instantiate the job object
    try:
        job = Job.from_config(config=config, target=target, tasks=tasks,
                              groups=groups, params=params)
    except CrossbowError as e:
        raise click.ClickException(str(e))

    job.show(output)
    if dry_run:
        return

    if fetch:
        queue.fetch()
    queue.put(job, prefix=job_prefix)

    if no_push:
        click.echo('Branches and commits created but not pushed: `{}`'
                   .format(job.branch))
    else:
        queue.push()
        click.echo('Pushed job identifier is: `{}`'.format(job.branch))


@crossbow.command()
@click.argument('task', required=True)
@click.option('--config-path', '-c',
              type=click.Path(exists=True), default=_default_config_path,
              help='Task configuration yml. Defaults to tasks.yml')
@click.option('--arrow-version', '-v', default=None,
              help='Set target version explicitly.')
@click.option('--arrow-remote', '-r', default=None,
              help='Set GitHub remote explicitly, which is going to be cloned '
                   'on the CI services. Note, that no validation happens '
                   'locally. Examples: https://github.com/apache/arrow or '
                   'https://github.com/kszucs/arrow.')
@click.option('--arrow-branch', '-b', default=None,
              help='Give the branch name explicitly, e.g. master, ARROW-1949.')
@click.option('--arrow-sha', '-t', default=None,
              help='Set commit SHA or Tag name explicitly, e.g. f67a515, '
                   'apache-arrow-0.11.1.')
@click.option('--param', '-p', 'params', multiple=True,
              help='Additional task parameters for rendering the CI templates')
@click.pass_obj
def render(obj, task, config_path, arrow_version, arrow_remote, arrow_branch,
           arrow_sha, params):
    """
    Utility command to check the rendered CI templates.
    """
    from .core import _flatten

    def highlight(code):
        try:
            from pygments import highlight
            from pygments.lexers import YamlLexer
            from pygments.formatters import TerminalFormatter
            return highlight(code, YamlLexer(), TerminalFormatter())
        except ImportError:
            return code

    arrow = obj['arrow']

    target = Target.from_repo(arrow, remote=arrow_remote, branch=arrow_branch,
                              head=arrow_sha, version=arrow_version)
    config = Config.load_yaml(config_path)
    params = dict([p.split("=") for p in params])
    params["queue_remote_url"] = "https://github.com/org/crossbow"
    job = Job.from_config(config=config, target=target, tasks=[task],
                          params=params)

    for task_name, rendered_files in job.render_tasks().items():
        for path, content in _flatten(rendered_files).items():
            click.echo('#' * 80)
            click.echo('### {:^72} ###'.format("/".join(path)))
            click.echo('#' * 80)
            click.echo(highlight(content))


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('--fetch/--no-fetch', default=True,
              help='Fetch references (branches and tags) from the remote')
@click.option('--task-filter', '-f', 'task_filters', multiple=True,
              help='Glob pattern for filtering relevant tasks')
@click.pass_obj
def status(obj, job_name, fetch, task_filters):
    output = obj['output']
    queue = obj['queue']
    if fetch:
        queue.fetch()
    job = queue.get(job_name)

    report = ConsoleReport(job, task_filters=task_filters)
    report.show(output)


@crossbow.command()
@click.argument('prefix', required=True)
@click.option('--fetch/--no-fetch', default=True,
              help='Fetch references (branches and tags) from the remote')
@click.pass_obj
def latest_prefix(obj, prefix, fetch):
    queue = obj['queue']
    if fetch:
        queue.fetch()
    latest = queue.latest_for_prefix(prefix)
    click.echo(latest.branch)


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('--sender-name', '-n',
              help='Name to use for report e-mail.')
@click.option('--sender-email', '-e',
              help='E-mail to use for report e-mail.')
@click.option('--recipient-email', '-r',
              help='Where to send the e-mail report')
@click.option('--smtp-user', '-u',
              help='E-mail address to use for SMTP login')
@click.option('--smtp-password', '-P',
              help='SMTP password to use for report e-mail.')
@click.option('--smtp-server', '-s', default='smtp.gmail.com',
              help='SMTP server to use for report e-mail.')
@click.option('--smtp-port', '-p', default=465,
              help='SMTP port to use for report e-mail.')
@click.option('--poll/--no-poll', default=False,
              help='Wait for completion if there are tasks pending')
@click.option('--poll-max-minutes', default=180,
              help='Maximum amount of time waiting for job completion')
@click.option('--poll-interval-minutes', default=10,
              help='Number of minutes to wait to check job status again')
@click.option('--send/--dry-run', default=False,
              help='Just display the report, don\'t send it')
@click.option('--fetch/--no-fetch', default=True,
              help='Fetch references (branches and tags) from the remote')
@click.pass_obj
def report(obj, job_name, sender_name, sender_email, recipient_email,
           smtp_user, smtp_password, smtp_server, smtp_port, poll,
           poll_max_minutes, poll_interval_minutes, send, fetch):
    """
    Send an e-mail report showing success/failure of tasks in a Crossbow run
    """
    output = obj['output']
    queue = obj['queue']
    if fetch:
        queue.fetch()

    job = queue.get(job_name)
    report = EmailReport(
        job=job,
        sender_name=sender_name,
        sender_email=sender_email,
        recipient_email=recipient_email
    )

    if poll:
        job.wait_until_finished(
            poll_max_minutes=poll_max_minutes,
            poll_interval_minutes=poll_interval_minutes
        )

    if send:
        report.send(
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            smtp_server=smtp_server,
            smtp_port=smtp_port
        )
    else:
        report.show(output)


@crossbow.command()
@click.argument('job-name', required=True)
@click.option('-t', '--target-dir',
              default=_default_arrow_path / 'packages',
              type=click.Path(file_okay=False, dir_okay=True),
              help='Directory to download the build artifacts')
@click.option('--dry-run/--execute', default=False,
              help='Just display process, don\'t download anything')
@click.option('--fetch/--no-fetch', default=True,
              help='Fetch references (branches and tags) from the remote')
@click.option('--task-filter', '-f', 'task_filters', multiple=True,
              help='Glob pattern for filtering relevant tasks')
@click.option('--validate-patterns/--skip-pattern-validation', default=True,
              help='Whether to validate artifact name patterns or not')
@click.pass_obj
def download_artifacts(obj, job_name, target_dir, dry_run, fetch,
                       validate_patterns, task_filters):
    """Download build artifacts from GitHub releases"""
    output = obj['output']

    # fetch the queue repository
    queue = obj['queue']
    if fetch:
        queue.fetch()

    # query the job's artifacts
    job = queue.get(job_name)

    # create directory to download the assets to
    target_dir = Path(target_dir).absolute() / job_name
    target_dir.mkdir(parents=True, exist_ok=True)

    # download the assets while showing the job status
    def asset_callback(task_name, task, asset):
        if asset is not None:
            path = target_dir / task_name / asset.name
            path.parent.mkdir(exist_ok=True)
            if not dry_run:
                import github3
                max_n_retries = 5
                n_retries = 0
                while True:
                    try:
                        asset.download(path)
                    except github3.exceptions.GitHubException as error:
                        n_retries += 1
                        if n_retries == max_n_retries:
                            raise
                        wait_seconds = 60
                        click.echo(f'Failed to download {path}')
                        click.echo(f'Retry #{n_retries} after {wait_seconds}s')
                        click.echo(error)
                        time.sleep(wait_seconds)
                    else:
                        break

    click.echo('Downloading {}\'s artifacts.'.format(job_name))
    click.echo('Destination directory is {}'.format(target_dir))
    click.echo()

    report = ConsoleReport(job, task_filters=task_filters)
    report.show(
        output,
        asset_callback=asset_callback,
        validate_patterns=validate_patterns
    )


@crossbow.command()
@click.argument('patterns', nargs=-1, required=True)
@click.option('--sha', required=True, help='Target committish')
@click.option('--tag', required=True, help='Target tag')
@click.option('--method', default='curl', help='Use cURL to upload')
@click.pass_obj
def upload_artifacts(obj, tag, sha, patterns, method):
    queue = obj['queue']
    queue.github_overwrite_release_assets(
        tag_name=tag, target_commitish=sha, method=method, patterns=patterns
    )
