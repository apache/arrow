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

from .reports import WorkflowReport, ChatReport
from ..crossbow.reports import ReportUtils


@click.group()
@click.option('--github-token', '-t', default=None,
              envvar=['GH_TOKEN'],
              help='OAuth token for GitHub authentication')
@click.option('--output-file', metavar='<output>',
              type=click.File('w', encoding='utf8'), default='-',
              help='Capture output result into file.')
@click.pass_context
def ci(ctx, github_token, output_file):
    """
    Tools for CI Extra jobs on GitHub actions.
    """
    ctx.ensure_object(dict)
    ctx.obj['github_token'] = github_token
    ctx.obj['output'] = output_file


@ci.command()
@click.argument('workflow_id', required=True)
@click.option('--send/--dry-run', default=False,
              help='Just display the report, don\'t send it.')
@click.option('--repository', '-r', default='apache/arrow',
              help='The repository where the workflow is located.')
@click.option('--ignore', '-i', default="",
              help='Job name to ignore from the list of jobs.')
@click.option('--webhook', '-w',
              help='Zulip/Slack Webhook address to send the report to.')
@click.option('--extra-message-success', '-s', default=None,
              help='Extra message, will be appended if no failures.')
@click.option('--extra-message-failure', '-f', default=None,
              help='Extra message, will be appended if there are failures.')
@click.pass_obj
def report_chat(obj, workflow_id, send, repository, ignore, webhook,
                extra_message_success, extra_message_failure):
    """
    Send a chat report to a webhook showing success/failure
    of tasks in a Crossbow run.
    """
    output = obj['output']

    report_chat = ChatReport(
        report=WorkflowReport(workflow_id, repository,
                              ignore_job=ignore, gh_token=obj['github_token']),
        extra_message_success=extra_message_success,
        extra_message_failure=extra_message_failure
    )
    if send:
        ReportUtils.send_message(webhook, report_chat.render("text"))
    else:
        output.write(report_chat.render("text"))
