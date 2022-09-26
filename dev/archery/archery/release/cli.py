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

import pathlib

import click

from ..utils.cli import validate_arrow_sources
from .core import Jira, CachedJira, Release


@click.group('release')
@click.option("--src", metavar="<arrow_src>", default=None,
              callback=validate_arrow_sources,
              help="Specify Arrow source directory.")
@click.option("--jira-cache", type=click.Path(), default=None,
              help="File path to cache queried JIRA issues per version.")
@click.pass_obj
def release(obj, src, jira_cache):
    """Release releated commands."""
    jira = Jira()
    if jira_cache is not None:
        jira = CachedJira(jira_cache, jira=jira)

    obj['jira'] = jira
    obj['repo'] = src.path


@release.command('curate', help="Lists release related Jira issues.")
@click.argument('version')
@click.option('--minimal/--full', '-m/-f',
              help="Only show actionable Jira issues.", default=False)
@click.pass_obj
def release_curate(obj, version, minimal):
    """Release curation."""
    release = Release.from_jira(version, jira=obj['jira'], repo=obj['repo'])
    curation = release.curate(minimal)

    click.echo(curation.render('console'))


@release.group('changelog')
def release_changelog():
    """Release changelog."""
    pass


@release_changelog.command('add')
@click.argument('version')
@click.pass_obj
def release_changelog_add(obj, version):
    """Prepend the changelog with the current release"""
    jira, repo = obj['jira'], obj['repo']

    # just handle the current version
    release = Release.from_jira(version, jira=jira, repo=repo)
    if release.is_released:
        raise ValueError('This version has been already released!')

    changelog = release.changelog()
    changelog_path = pathlib.Path(repo) / 'CHANGELOG.md'

    current_content = changelog_path.read_text()
    new_content = changelog.render('markdown') + current_content

    changelog_path.write_text(new_content)
    click.echo("CHANGELOG.md is updated!")


@release_changelog.command('generate')
@click.argument('version')
@click.argument('output', type=click.File('w', encoding='utf8'), default='-')
@click.pass_obj
def release_changelog_generate(obj, version, output):
    """Generate the changelog of a specific release."""
    jira, repo = obj['jira'], obj['repo']

    # just handle the current version
    release = Release.from_jira(version, jira=jira, repo=repo)

    changelog = release.changelog()
    output.write(changelog.render('markdown'))


@release_changelog.command('regenerate')
@click.pass_obj
def release_changelog_regenerate(obj):
    """Regeneretate the whole CHANGELOG.md file"""
    jira, repo = obj['jira'], obj['repo']
    changelogs = []

    for version in jira.project_versions('ARROW'):
        if not version.released:
            continue
        release = Release.from_jira(version, jira=jira, repo=repo)
        click.echo('Querying changelog for version: {}'.format(version))
        changelogs.append(release.changelog())

    click.echo('Rendering new CHANGELOG.md file...')
    changelog_path = pathlib.Path(repo) / 'CHANGELOG.md'
    with changelog_path.open('w') as fp:
        for cl in changelogs:
            fp.write(cl.render('markdown'))


@release.command('cherry-pick')
@click.argument('version')
@click.option('--dry-run/--execute', default=True,
              help="Display the git commands instead of executing them.")
@click.option('--recreate/--continue', default=True,
              help="Recreate the maintenance branch or only apply unapplied "
                   "patches.")
@click.pass_obj
def release_cherry_pick(obj, version, dry_run, recreate):
    """
    Cherry pick commits.
    """
    release = Release.from_jira(version, jira=obj['jira'], repo=obj['repo'])

    if not dry_run:
        release.cherry_pick_commits(recreate_branch=recreate)
    else:
        click.echo(f'git checkout -b {release.branch} {release.base_branch}')
        for commit in release.commits_to_pick():
            click.echo('git cherry-pick {}'.format(commit.hexsha))
