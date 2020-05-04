#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utility for generating changelogs for fix versions
# requirements: pip install jira
# Set $JIRA_USERNAME, $JIRA_PASSWORD environment variables

from __future__ import print_function

from collections import defaultdict
from datetime import datetime
from io import StringIO
import locale
import os
import re
import sys

import jira.client

# ASF JIRA username
JIRA_USERNAME = os.environ["APACHE_JIRA_USERNAME"]
# ASF JIRA password
JIRA_PASSWORD = os.environ["APACHE_JIRA_PASSWORD"]

JIRA_API_BASE = "https://issues.apache.org/jira"

asf_jira = jira.client.JIRA(options={'server': JIRA_API_BASE},
                            basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))


locale.setlocale(locale.LC_ALL, 'en_US.utf-8')


release_dir = os.path.realpath(os.path.dirname(__file__))
ARROW_ROOT_DEFAULT = os.path.join(release_dir, '..', '..')
ARROW_ROOT = os.environ.get("ARROW_ROOT", ARROW_ROOT_DEFAULT)


def get_issues_for_version(version):
    jql = ("project=ARROW "
           "AND fixVersion='{0}' "
           "AND status = Resolved "
           "AND resolution in (Fixed, Done) "
           "ORDER BY issuetype DESC").format(version)

    return asf_jira.search_issues(jql, maxResults=9999)


def get_last_major_version(current_version):
    # TODO: This doesn't work for generating a changelog for the _first_ major
    # release, but we probably don't care
    major_versions = [
        v for v in asf_jira.project('ARROW').versions
        if v.name[0].isdigit() and v.name.split('.')[-1] == '0'
    ]

    # Sort the versions
    def sort_version(x):
        major, minor, patch = x.name.split('.')
        return int(major), int(minor)

    major_versions.sort(key=sort_version)

    # Find index of version being released
    current_version_index = ([x.name for x in major_versions]
                             .index(current_version))

    return major_versions[current_version_index - 1]


def get_jiras_from_git_changelog(current_version):
    # We use this to get the resolved PARQUET JIRAs
    from subprocess import check_output

    last_major_version = get_last_major_version(current_version)

    # Path to .git directory
    git_dir = os.path.join(ARROW_ROOT, '.git')

    cmd = ['git', '--git-dir', git_dir, 'log', '--pretty=format:%s',
           'apache-arrow-{}..apache-arrow-{}'.format(last_major_version,
                                                     current_version)]
    output = check_output(cmd).decode('utf-8')

    resolved_jiras = []
    regex = re.compile(r'[a-zA-Z]+-[0-9]+')
    for desc in output.splitlines():
        maybe_jira = desc.split(':')[0]

        # Sometimes people forget the colon
        maybe_jira = maybe_jira.split(' ')[0]
        if regex.match(maybe_jira):
            resolved_jiras.append(maybe_jira)

    return resolved_jiras


LINK_TEMPLATE = '[{0}](https://issues.apache.org/jira/browse/{0})'


def format_changelog_markdown(issues, out):
    issues_by_type = defaultdict(list)
    for issue in issues:
        issues_by_type[issue.fields.issuetype.name].append(issue)

    for issue_type, issue_group in sorted(issues_by_type.items()):
        issue_group.sort(key=lambda x: x.key)

        out.write('## {0}\n\n'.format(_escape_for_markdown(issue_type)))
        for issue in issue_group:
            markdown_summary = _escape_for_markdown(issue.fields.summary)
            out.write('* {0} - {1}\n'.format(issue.key,
                                             markdown_summary))
        out.write('\n')


def _escape_for_markdown(x):
    return (
        x.replace('_', r'\_')  # underscores
        .replace('`', r'\`')   # backticks
        .replace('*', r'\*')   # asterisks
    )


def format_changelog_website(issues, out):
    NEW_FEATURE = 'New Features and Improvements'
    BUGFIX = 'Bug Fixes'

    CATEGORIES = {
        'New Feature': NEW_FEATURE,
        'Improvement': NEW_FEATURE,
        'Wish': NEW_FEATURE,
        'Task': NEW_FEATURE,
        'Test': BUGFIX,
        'Bug': BUGFIX,
        'Sub-task': NEW_FEATURE
    }

    issues_by_category = defaultdict(list)
    for issue in issues:
        issue_type = issue.fields.issuetype.name
        website_category = CATEGORIES[issue_type]
        issues_by_category[website_category].append(issue)

    WEBSITE_ORDER = [NEW_FEATURE, BUGFIX]

    for issue_category in WEBSITE_ORDER:
        issue_group = issues_by_category[issue_category]
        issue_group.sort(key=lambda x: x.key)

        out.write('## {0}\n\n'.format(issue_category))
        for issue in issue_group:
            name = LINK_TEMPLATE.format(issue.key)
            markdown_summary = _escape_for_markdown(issue.fields.summary)
            out.write('* {0} - {1}\n'
                      .format(name, markdown_summary))
        out.write('\n')


def get_resolved_parquet_issues(version):
    git_resolved_jiras = set(get_jiras_from_git_changelog(version))

    # We don't assume that resolved Parquet issues are found in a single Fix
    # Version, so for now we query them all and then select only the ones that
    # are found in the git log
    jql = ("project=PARQUET "
           "AND component='parquet-cpp' "
           "AND status = Resolved "
           "AND resolution in (Fixed, Done) "
           "ORDER BY issuetype DESC")

    all_issues = asf_jira.search_issues(jql, maxResults=9999)
    return [issue for issue in all_issues if issue.key in git_resolved_jiras]


def get_changelog(version, for_website=False):
    issues_for_version = get_issues_for_version(version)

    # Infer resolved Parquet issues, since these can only really be known by
    # looking at the git log
    parquet_issues = get_resolved_parquet_issues(version)
    issues_for_version.extend(parquet_issues)

    buf = StringIO()

    if for_website:
        format_changelog_website(issues_for_version, buf)
    else:
        format_changelog_markdown(issues_for_version, buf)

    return buf.getvalue()


def append_changelog(version, changelog_path):
    new_changelog = get_changelog(version)

    with open(changelog_path, 'r') as f:
        old_changelog = f.readlines()

    result = StringIO()
    # Header
    print(''.join(old_changelog[:18]), file=result)

    # New version
    today = datetime.today().strftime('%d %B %Y')
    print('# Apache Arrow {0} ({1})'.format(version, today),
          end='', file=result)
    print('\n', file=result)
    print(new_changelog, end='', file=result)

    # Prior versions
    print(''.join(old_changelog[19:]), file=result)

    with open(changelog_path, 'w') as f:
        f.write(result.getvalue().rstrip() + '\n')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: changelog.py $FIX_VERSION [$IS_WEBSITE] '
              '[$CHANGELOG_TO_UPDATE]')

    for_website = len(sys.argv) > 2 and sys.argv[2] == '1'

    version = sys.argv[1]

    if len(sys.argv) > 3:
        changelog_path = sys.argv[3]
        append_changelog(version, changelog_path)
    else:
        print(get_changelog(version, for_website=for_website))
