import shlex
from functools import partial

import click
import github


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
        except EventError:
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
            pull.create_issue_comment("```\n{}\n```".format(e.message))
        except Exception:
            comment.create_reaction('-1')
        else:
            comment.create_reaction('+1')

    def handle_review_comment(self, payload):
        raise NotImplementedError()


command = partial(click.command, cls=Command)
group = partial(click.group, cls=Group)


@group(name='@ursabot')
@click.pass_context
def bot(ctx):
    """Ursabot"""
    ctx.ensure_object(dict)


# @ursabot.command()
# @click.argument('baseline', type=str, metavar='[<baseline>]', default=None,
#                 required=False)
# @click.option('--suite-filter', metavar='<regex>', show_default=True,
#               type=str, default=None, help='Regex filtering benchmark suites.')
# @click.option('--benchmark-filter', metavar='<regex>', show_default=True,
#               type=str, default=None,
#               help='Regex filtering benchmarks.')
# def benchmark(baseline, suite_filter, benchmark_filter):
#     """Run the benchmark suite in comparison mode.

#     This command will run the benchmark suite for tip of the branch commit
#     against `<baseline>` (or master if not provided).

#     Examples:

#     \b
#     # Run the all the benchmarks
#     @ursabot benchmark

#     \b
#     # Compare only benchmarks where the name matches the /^Sum/ regex
#     @ursabot benchmark --benchmark-filter=^Sum

#     \b
#     # Compare only benchmarks where the suite matches the /compute-/ regex.
#     # A suite is the C++ binary.
#     @ursabot benchmark --suite-filter=compute-

#     \b
#     # Sometimes a new optimization requires the addition of new benchmarks to
#     # quantify the performance increase. When doing this be sure to add the
#     # benchmark in a separate commit before introducing the optimization.
#     #
#     # Note that specifying the baseline is the only way to compare using a new
#     # benchmark, since master does not contain the new benchmark and no
#     # comparison is possible.
#     #
#     # The following command compares the results of matching benchmarks,
#     # compiling against HEAD and the provided baseline commit, e.g. eaf8302.
#     # You can use this to quantify the performance improvement of new
#     # optimizations or to check for regressions.
#     @ursabot benchmark --benchmark-filter=MyBenchmark eaf8302
#     """
#     # each command must return a dictionary which are set as build properties
#     props = {'command': 'benchmark'}

#     if baseline:
#         props['benchmark_baseline'] = baseline

#     opts = []
#     if suite_filter:
#         suite_filter = shlex.quote(suite_filter)
#         opts.append(f'--suite-filter={suite_filter}')
#     if benchmark_filter:
#         benchmark_filter = shlex.quote(benchmark_filter)
#         opts.append(f'--benchmark-filter={benchmark_filter}')

#     if opts:
#         props['benchmark_options'] = opts

#     return props


@bot.group()
@click.option('--repo', '-r', default='ursa-labs/crossbow',
              help='Crossbow repository on github to use')
@click.pass_obj
def crossbow(props, repo):
    """Trigger crossbow builds for this pull request"""
    # TODO(kszucs): validate the repo format
    props['command'] = 'crossbow'
    props['crossbow_repo'] = repo  # github user/repo
    props['crossbow_repository'] = f'https://github.com/{repo}'  # git url


@crossbow.command()
@click.argument('task', nargs=-1, required=False)
@click.option('--group', '-g', multiple=True,
              help='Submit task groups as defined in tests.yml')
@click.pass_obj
def submit(props, task, group):
    """Submit crossbow testing tasks.

    See groups defined in arrow/dev/tasks/tests.yml
    """
    args = ['-c', 'tasks.yml']
    for g in group:
        args.extend(['-g', g])
    for t in task:
        args.append(t)

    return {'crossbow_args': args, **props}