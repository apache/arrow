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
import click

from ursabot.commands import group


@group(name='@ursabot')
@click.pass_context
def ursabot(ctx):
    """Ursabot"""
    ctx.ensure_object(dict)


@ursabot.command()
def build():
    """Trigger all tests registered for this pull request."""
    # each command must return a dictionary which are set as build properties
    return {'command': 'build'}


@ursabot.command()
@click.argument('baseline', type=str, metavar='[<baseline>]', default=None,
                required=False)
@click.option('--suite-filter', metavar='<regex>', show_default=True,
              type=str, default=None, help='Regex filtering benchmark suites.')
@click.option('--benchmark-filter', metavar='<regex>', show_default=True,
              type=str, default=None,
              help='Regex filtering benchmarks.')
def benchmark(baseline, suite_filter, benchmark_filter):
    """Run the benchmark suite in comparison mode.

    This command will run the benchmark suite for tip of the branch commit
    against `<baseline>` (or master if not provided).

    Examples:

    \b
    # Run the all the benchmarks
    @ursabot benchmark

    \b
    # Compare only benchmarks where the name matches the /^Sum/ regex
    @ursabot benchmark --benchmark-filter=^Sum

    \b
    # Compare only benchmarks where the suite matches the /compute-/ regex.
    # A suite is the C++ binary.
    @ursabot benchmark --suite-filter=compute-

    \b
    # Sometimes a new optimization requires the addition of new benchmarks to
    # quantify the performance increase. When doing this be sure to add the
    # benchmark in a separate commit before introducing the optimization.
    #
    # Note that specifying the baseline is the only way to compare using a new
    # benchmark, since master does not contain the new benchmark and no
    # comparison is possible.
    #
    # The following command compares the results of matching benchmarks,
    # compiling against HEAD and the provided baseline commit, e.g. eaf8302.
    # You can use this to quantify the performance improvement of new
    # optimizations or to check for regressions.
    @ursabot benchmark --benchmark-filter=MyBenchmark eaf8302
    """
    # each command must return a dictionary which are set as build properties
    props = {'command': 'benchmark'}

    if baseline:
        props['benchmark_baseline'] = baseline

    opts = []
    if suite_filter:
        suite_filter = shlex.quote(suite_filter)
        opts.append(f'--suite-filter={suite_filter}')
    if benchmark_filter:
        benchmark_filter = shlex.quote(benchmark_filter)
        opts.append(f'--benchmark-filter={benchmark_filter}')

    if opts:
        props['benchmark_options'] = opts

    return props


@ursabot.group()
@click.option('--repo', '-r', default='ursa-labs/crossbow',
              help='Crossbow repository on github to use')
@click.pass_obj
def crossbow(props, repo):
    """Trigger crossbow builds for this pull request"""
    props['command'] = 'crossbow'
    props['crossbow_repo'] = f'https://github.com/{repo}'


@crossbow.command()
@click.argument('task', nargs=-1, required=False)
@click.option('--group', '-g', multiple=True,
              type=click.Choice(['docker', 'integration', 'cpp-python']),
              help='Submit task groups as defined in tests.yml')
@click.pass_obj
def test(props, task, group):
    """Submit crossbow testing tasks.

    See groups defined in arrow/dev/tasks/tests.yml
    """
    args = ['-c', 'tests.yml']
    for g in group:
        args.extend(['-g', g])
    for t in task:
        args.append(t)

    return {'crossbow_args': args, **props}


@crossbow.command()
@click.argument('task', nargs=-1, required=False)
@click.option('--group', '-g', multiple=True,
              type=click.Choice(['conda', 'wheel', 'linux', 'gandiva']),
              help='Submit task groups as defined in tasks.yml')
@click.pass_obj
def package(props, task, group):
    """Submit crossbow packaging tasks.

    See groups defined in arrow/dev/tasks/tasks.yml
    """
    args = ['-c', 'tasks.yml']
    for g in group:
        args.extend(['-g', g])
    for t in task:
        args.append(t)

    return {'crossbow_args': args, **props}
