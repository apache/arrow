import json
from pathlib import Path
from unittest.mock import Mock

import click
import pytest
import responses as rsps
from archery.bot import CommentBot, CommandError, group


@pytest.fixture
def responses():
    with rsps.RequestsMock() as mock:
        yield mock


def load_fixture(name):
    path = Path(__file__).parent / 'fixtures' / '{}.json'.format(name)
    with path.open('r') as fp:
        return json.load(fp)


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
    # the bot is not mentiond, nothing to do
    'event-issue-comment-not-mentioning-ursabot',
    # don't respond to itself, it prevents recursive comment storms!
    'event-issue-comment-by-ursabot',
    # non-authorized user sent the comment, do not respond
    'event-issue-comment-by-non-authorized-user',
])
def test_noop_events(fixture_name):
    payload = load_fixture(fixture_name)

    handler = Mock()
    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    handler.assert_not_called()


def test_issue_comment_without_pull_request(responses):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/19'),
        json=load_fixture('issue-19'),
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

    payload = load_fixture('event-issue-comment-without-pull-request')
    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    post = responses.calls[2]
    assert json.loads(post.request.body) == {
        'body': "The comment bot only listens to pull request comments!"
    }


def test_respond_with_usage(responses):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/26'),
        json=load_fixture('issue-26'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/pulls/26'),
        json=load_fixture('pull-request-26'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/comments/480243811'),
        json=load_fixture('issue-comment-480243811')
    )
    responses.add(
        responses.POST,
        github_url('/repos/ursa-labs/ursabot/issues/26/comments'),
        json={}
    )

    def handler(command, **kwargs):
        raise CommandError('test-usage')

    payload = load_fixture('event-issue-comment-with-empty-command')
    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    post = responses.calls[3]
    assert json.loads(post.request.body) == {'body': '```\ntest-usage\n```'}


@pytest.mark.parametrize(('command', 'reaction'), [
    ('@ursabot build', '+1'),
    ('@ursabot listen', '-1'),
])
def test_issue_comment_with_commands(responses, command, reaction):
    responses.add(
        responses.GET,
        github_url('/repositories/169101701/issues/26'),
        json=load_fixture('issue-26'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/pulls/26'),
        json=load_fixture('pull-request-26'),
        status=200
    )
    responses.add(
        responses.GET,
        github_url('/repos/ursa-labs/ursabot/issues/comments/480248726'),
        json=load_fixture('issue-comment-480248726')
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

    payload = load_fixture('event-issue-comment-build-command')
    payload["comment"]["body"] = command

    bot = CommentBot(name='ursabot', token='', handler=handler)
    bot.handle('issue_comment', payload)

    post = responses.calls[3]
    assert json.loads(post.request.body) == {'content': reaction}