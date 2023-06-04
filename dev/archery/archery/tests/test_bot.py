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
import os
from unittest import mock
from unittest.mock import Mock

import click
import pytest
import responses as rsps

from archery.bot import (
    CommentBot,
    CommandError,
    PullRequestState,
    PullRequestWorkflowBot,
    group
)


@pytest.fixture
def responses():
    with rsps.RequestsMock() as mock:
        yield mock


@pytest.fixture(autouse=True)
def set_env_vars():
    with mock.patch.dict(os.environ, {
        "GITHUB_SERVER_URL": "https://github.com",
        "GITHUB_REPOSITORY": "apache/arrow",
        "GITHUB_RUN_ID": "1463784188"
    }):
        yield


def github_url(path):
    return 'https://api.github.com:443/{}'.format(path.strip('/'))


@group()
def custom_handler():
    pass


@custom_handler.command()
@click.pass_obj
def extra(obj):
    return obj


@custom_handler.command()
@click.option('--force', '-f', is_flag=True)
def build(force):
    return force


@custom_handler.command()
@click.option('--name', required=True)
def benchmark(name):
    return name


def test_click_based_commands():
    assert custom_handler('build') is False
    assert custom_handler('build -f') is True

    assert custom_handler('benchmark --name strings') == 'strings'
    with pytest.raises(CommandError):
        assert custom_handler('benchmark')

    assert custom_handler('extra', extra='data') == {'extra': 'data'}


@pytest.mark.parametrize('fixture_name', [
    # the bot is not mentioned, nothing to do
    'event-issue-comment-not-mentioning-ursabot.json',
    # don't respond to itself, it prevents recursive comment storms!
    'event-issue-comment-by-ursabot.json',
])
def test_noop_events(load_fixture, fixture_name):
    payload = load_fixture(fixture_name)

    handler = Mock()
    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    handler.assert_not_called()


def test_unathorized_user_comment(load_fixture, responses):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/26'),
        json=load_fixture('issue-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/pulls/26'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/comments/480243815'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.POST,
        github_url('/repos/ursa-labs/ursabot/issues/26/comments'),
        json={}
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/pulls/26/reactions'),
        json=()
    )

    def handler(command, **kwargs):
        pass

    payload = load_fixture('event-issue-comment-by-non-authorized-user.json')
    payload["comment"]["body"] = '@ursabot crossbow submit -g nightly'
    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    print([c.request.body for c in responses.calls])
    post = responses.calls[-2]
    reaction = responses.calls[-1]
    comment = ("```\nOnly contributors can submit requests to this bot. "
               "Please ask someone from the community for help with getting "
               "the first commit in.\n"
               "The Archery job run can be found at: "
               "https://github.com/apache/arrow/actions/runs/1463784188\n"
               "```")
    assert json.loads(post.request.body) == {
        "body": f'{comment}'}
    assert json.loads(reaction.request.body) == {'content': '-1'}


def test_issue_comment_without_pull_request(load_fixture, responses):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/19'),
        json=load_fixture('issue-19.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('repos/ursa-labs/ursabot/pulls/19'),
        json={},
        status=404
    )
    responses.add(
        responses.POST,
        github_url('/repos/ursa-labs/ursabot/issues/19/comments'),
        json={}
    )

    def handler(command, **kwargs):
        pass

    payload = load_fixture('event-issue-comment-without-pull-request.json')
    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    post = responses.calls[2]
    assert json.loads(post.request.body) == {
        'body': "The comment bot only listens to pull request comments!"
    }


def test_respond_with_usage(load_fixture, responses):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/26'),
        json=load_fixture('issue-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/pulls/26'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/comments/480243811'),
        json=load_fixture('issue-comment-480243811.json')
    )
    responses.add(
        responses.POST,
        github_url('/repos/ursa-labs/ursabot/issues/26/comments'),
        json={}
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/comments/479081273/reactions'),
        json=()
    )

    def handler(command, **kwargs):
        raise CommandError('test-usage')

    payload = load_fixture('event-issue-comment-with-empty-command.json')
    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    post = responses.calls[3]
    assert json.loads(post.request.body) == \
        {'body':
         ("```\ntest-usage\n"
          "The Archery job run can be found at: "
          "https://github.com/apache/arrow/actions/runs/1463784188\n"
          "```")
         }


@pytest.mark.parametrize(('command', 'reaction'), [
    ('@ursabot build', '+1'),
    ('@ursabot build\nwith a comment', '+1'),
])
def test_issue_comment_with_commands(load_fixture, responses, command,
                                     reaction):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/26'),
        json=load_fixture('issue-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/pulls/26'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/comments/480248726'),
        json=load_fixture('issue-comment-480248726.json')
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/comments/480248726/reactions'
        ),
        json={}
    )

    def handler(command, **kwargs):
        if command == 'build':
            return True
        else:
            raise ValueError('Only `build` command is supported.')

    payload = load_fixture('event-issue-comment-build-command.json')
    payload["comment"]["body"] = command

    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    post = responses.calls[3]
    assert json.loads(post.request.body) == {'content': reaction}


@pytest.mark.parametrize(('command', 'reaction'), [
    ('@ursabot listen', '-1'),
])
def test_issue_comment_invalid_commands(load_fixture, responses, command,
                                        reaction):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/26'),
        json=load_fixture('issue-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/pulls/26'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/comments/480248726'),
        json=load_fixture('issue-comment-480248726.json')
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/comments/480248726/reactions'
        ),
        json={}
    )
    responses.add(
        responses.POST,
        github_url('/repos/ursa-labs/ursabot/issues/26/comments'),
        json={}
    )

    def handler(command, **kwargs):
        if command == 'build':
            return True
        else:
            raise ValueError('Only `build` command is supported.')

    payload = load_fixture('event-issue-comment-build-command.json')
    payload["comment"]["body"] = command

    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    # Setting reaction is always the last call
    post = responses.calls[-1]
    assert json.loads(post.request.body) == {'content': reaction}


def test_issue_comment_with_commands_bot_not_first(load_fixture, responses):
    # when the @-mention is not first, this is a no-op
    handler = Mock()

    payload = load_fixture('event-issue-comment-build-command.json')
    payload["comment"]["body"] = 'with a comment\n@ursabot build'

    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    handler.assert_not_called()


@pytest.mark.parametrize(('fixture_name', 'expected_label'), [
    ('event-pull-request-target-opened-committer.json',
     PullRequestState.committer_review.value),
    ('event-pull-request-target-opened-non-committer.json',
     PullRequestState.review.value),
])
def test_open_pull_request(load_fixture, responses, fixture_name, expected_label):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=[],
        status=200
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/26/labels'
        ),
        status=201
    )
    payload = load_fixture(fixture_name)

    bot = PullRequestWorkflowBot('pull_request_target', payload, token='')
    bot.handle()

    # Setting awaiting committer review or awaiting review label
    post = responses.calls[-1]
    assert json.loads(post.request.body) == [expected_label]


@pytest.mark.parametrize(('fixture_name', 'expected_label'), [
    ('event-pull-request-target-opened-non-committer.json',
     PullRequestState.committer_review.value),
])
def test_open_pull_request_with_committer_list(load_fixture, responses, fixture_name,
                                               expected_label):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=[],
        status=200
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/26/labels'
        ),
        status=201
    )
    payload = load_fixture(fixture_name)

    # Even though the author_association is not committer the list overrides.
    bot = PullRequestWorkflowBot(
        'pull_request_target', payload, token='', committers=['kszucs'])
    bot.handle()

    # Setting awaiting committer review or awaiting review label
    post = responses.calls[-1]
    assert json.loads(post.request.body) == [expected_label]


@pytest.mark.parametrize(('fixture_name', 'expected_label'), [
    ('event-pull-request-target-opened-committer.json',
     PullRequestState.committer_review.value),
])
def test_open_pull_request_with_existing_label(
        load_fixture, responses, fixture_name, expected_label):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=load_fixture('label-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.DELETE,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels/awaiting%20review'),
        status=200
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/26/labels'
        ),
        status=201
    )
    payload = load_fixture(fixture_name)
    payload['pull_request']['labels'] = ['awaiting review']

    bot = PullRequestWorkflowBot('pull_request_target', payload, token='')
    bot.handle()

    post = responses.calls[-1]
    assert json.loads(post.request.body) == [expected_label]


@pytest.mark.parametrize(('fixture_name', 'review_state', 'expected_label'), [
    ('event-pr-review-committer.json', 'commented', PullRequestState.changes.value),
    ('event-pr-review-committer.json', 'changes_requested',
     PullRequestState.changes.value),
    ('event-pr-review-committer.json', 'approved', PullRequestState.merge.value),
    ('event-pr-review-non-committer.json', 'commented',
     PullRequestState.committer_review.value),
    ('event-pr-review-non-committer.json', 'changes_requested',
     PullRequestState.committer_review.value),
    ('event-pr-review-non-committer.json', 'approved',
     PullRequestState.committer_review.value),
])
def test_pull_request_review_awaiting_review(
        load_fixture, responses, fixture_name, review_state, expected_label):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=load_fixture('label-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.DELETE,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels/awaiting%20review'),
        status=200
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/26/labels'
        ),
        status=201
    )
    payload = load_fixture(fixture_name)
    payload['pull_request']['labels'] = ['awaiting review']
    payload['review']['state'] = review_state

    bot = PullRequestWorkflowBot('pull_request_review', payload, token='')
    bot.handle()

    post = responses.calls[-1]
    assert json.loads(post.request.body) == [expected_label]


@pytest.mark.parametrize(('review_state', 'expected_label'), [
    ('commented', PullRequestState.changes.value),
    ('changes_requested', PullRequestState.changes.value),
    ('approved', PullRequestState.merge.value),
])
def test_pull_request_committer_review_awaiting_change_review(
        load_fixture, responses, review_state, expected_label):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=load_fixture('label-awaiting-change-review.json'),
        status=200
    )
    responses.add(
        responses.DELETE,
        github_url('/repos/ursa-labs/ursabot/issues/26/' +
                   'labels/awaiting%20change%20review'),
        status=200
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/26/labels'
        ),
        status=201
    )
    payload = load_fixture('event-pr-review-committer.json')
    payload['pull_request']['labels'] = ['awaiting change review']
    payload['review']['state'] = review_state

    bot = PullRequestWorkflowBot('pull_request_review', payload, token='')
    bot.handle()

    post = responses.calls[-1]
    assert json.loads(post.request.body) == [expected_label]


@pytest.mark.parametrize('review_state', [
    'commented', 'changes_requested', 'approved'])
def test_pull_request_non_committer_review_awaiting_change_review(
        load_fixture, responses, review_state):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=load_fixture('label-awaiting-change-review.json'),
        status=200
    )
    payload = load_fixture('event-pr-review-non-committer.json')
    payload['pull_request']['labels'] = ['awaiting change review']
    payload['review']['state'] = review_state

    bot = PullRequestWorkflowBot('pull_request_review', payload, token='')
    bot.handle()

    # No requests to delete post new labels on non-committer reviews
    assert len(responses.calls) == 2


def test_pull_request_synchronize_event_on_awaiting_changes(
        load_fixture, responses):
    payload = load_fixture('event-pull-request-target-synchronize.json')

    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=load_fixture('label-awaiting-changes.json'),
        status=200
    )
    responses.add(
        responses.DELETE,
        github_url('/repos/ursa-labs/ursabot/issues/26/' +
                   'labels/awaiting%20changes'),
        status=200
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/26/labels'
        ),
        status=201
    )

    bot = PullRequestWorkflowBot('pull_request_target', payload, token='')
    bot.handle()
    # after push event label changes.
    post = responses.calls[-1]
    assert json.loads(post.request.body) == ["awaiting change review"]


def test_pull_request_synchronize_event_on_awaiting_review(
        load_fixture, responses):
    payload = load_fixture('event-pull-request-target-synchronize.json')

    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26-awaiting-review.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=load_fixture('label-awaiting-review.json'),
        status=200
    )

    bot = PullRequestWorkflowBot('pull_request_target', payload, token='')
    bot.handle()
    # No requests to delete or post new labels on push awaiting review
    assert len(responses.calls) == 2


def test_pull_request_synchronize_event_on_existing_pr_without_state(
        load_fixture, responses):
    payload = load_fixture('event-pull-request-target-synchronize.json')

    responses.add(
        responses.GET,
        github_url('/repositories/169101701/pulls/26'),
        json=load_fixture('pull-request-26.json'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/26/labels'),
        json=[],
        status=200
    )
    responses.add(
        responses.POST,
        github_url(
            '/repos/ursa-labs/ursabot/issues/26/labels'
        ),
        status=201
    )

    bot = PullRequestWorkflowBot('pull_request_target', payload, token='')
    bot.handle()
    # after push event label get set to default
    post = responses.calls[-1]
    assert json.loads(post.request.body) == ["awaiting review"]
