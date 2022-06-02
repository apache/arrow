#!/usr/bin/env python3

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

# Utility for creating well-formed pull request merges and pushing them to
# Apache.
#   usage: ./merge_arrow_pr.py  <pr-number>  (see config env vars below)
#
# This utility assumes:
#   - you already have a local Arrow git clone
#   - you have added remotes corresponding to both:
#       (i) the GitHub Apache Arrow mirror
#       (ii) the Apache git repo
#
# There are several pieces of authorization possibly needed via environment
# variables.
#
# Configuration environment variables:
#   - APACHE_JIRA_TOKEN: your Apache JIRA Personal Access Token
#   - ARROW_GITHUB_API_TOKEN: a GitHub API token to use for API requests
#   - PR_REMOTE_NAME: the name of the remote to the Apache git repo (set to
#                     'apache' by default)
#   - DEBUG: use for testing to avoid pushing to apache (0 by default)

import configparser
import os
import pprint
import re
import subprocess
import sys
import requests
import getpass

from six.moves import input
import six

try:
    import jira.client
    import jira.exceptions
except ImportError:
    print("Could not find jira library. "
          "Run 'sudo pip install jira' to install.")
    print("Exiting without trying to close the associated JIRA.")
    sys.exit(1)

# Remote name which points to the GitHub site
PR_REMOTE_NAME = os.environ.get("PR_REMOTE_NAME", "apache")

# For testing to avoid accidentally pushing to apache
DEBUG = bool(int(os.environ.get("DEBUG", 0)))


if DEBUG:
    print("**************** DEBUGGING ****************")


JIRA_API_BASE = "https://issues.apache.org/jira"


def get_json(url, headers=None):
    response = requests.get(url, headers=headers)
    return response.json()


def run_cmd(cmd):
    if isinstance(cmd, six.string_types):
        cmd = cmd.split(' ')

    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as e:
        # this avoids hiding the stdout / stderr of failed processes
        print('Command failed: %s' % cmd)
        print('With output:')
        print('--------------')
        print(e.output)
        print('--------------')
        raise e

    if isinstance(output, six.binary_type):
        output = output.decode('utf-8')
    return output


_REGEX_CI_DIRECTIVE = re.compile(r'\[[^\]]*\]')


def strip_ci_directives(commit_message):
    # Remove things like '[force ci]', '[skip appveyor]' from the assembled
    # commit message
    return _REGEX_CI_DIRECTIVE.sub('', commit_message)


def fix_version_from_branch(branch, versions):
    # Note: Assumes this is a sorted (newest->oldest) list of un-released
    # versions
    if branch == "master":
        return versions[-1]
    else:
        branch_ver = branch.replace("branch-", "")
        return [x for x in versions if x.name.startswith(branch_ver)][-1]


# We can merge both ARROW and PARQUET patchesa
SUPPORTED_PROJECTS = ['ARROW', 'PARQUET']
PR_TITLE_REGEXEN = [(project, re.compile(r'^(' + project + r'-[0-9]+)\b.*$'))
                    for project in SUPPORTED_PROJECTS]


class JiraIssue(object):

    def __init__(self, jira_con, jira_id, project, cmd):
        self.jira_con = jira_con
        self.jira_id = jira_id
        self.project = project
        self.cmd = cmd

        try:
            self.issue = jira_con.issue(jira_id)
        except Exception as e:
            self.cmd.fail("ASF JIRA could not find %s\n%s" % (jira_id, e))

    @property
    def current_fix_versions(self):
        return self.issue.fields.fixVersions

    def get_candidate_fix_versions(self, merge_branches=('master',)):
        # Only suggest versions starting with a number, like 0.x but not JS-0.x
        all_versions = self.jira_con.project_versions(self.project)
        unreleased_versions = [x for x in all_versions
                               if not x.raw['released']]

        unreleased_versions = sorted(unreleased_versions,
                                     key=lambda x: x.name, reverse=True)

        mainline_versions = self._filter_mainline_versions(unreleased_versions)

        mainline_non_patch_versions = []
        for v in mainline_versions:
            (major, minor, patch) = v.name.split(".")
            if patch == "0":
                mainline_non_patch_versions.append(v)

        if len(mainline_versions) > len(mainline_non_patch_versions):
            # If there is a non-patch release, suggest that instead
            mainline_versions = mainline_non_patch_versions

        default_fix_versions = [
            fix_version_from_branch(x, mainline_versions).name
            for x in merge_branches]

        return all_versions, default_fix_versions

    def _filter_mainline_versions(self, versions):
        if self.project == 'PARQUET':
            mainline_regex = re.compile(r'cpp-\d.*')
        else:
            mainline_regex = re.compile(r'\d.*')

        return [x for x in versions if mainline_regex.match(x.name)]

    def resolve(self, fix_versions, comment):
        fields = self.issue.fields
        cur_status = fields.status.name

        if cur_status == "Resolved" or cur_status == "Closed":
            self.cmd.fail("JIRA issue %s already has status '%s'"
                          % (self.jira_id, cur_status))

        if DEBUG:
            print("JIRA issue %s untouched" % (self.jira_id))
            return

        resolve = [x for x in self.jira_con.transitions(self.jira_id)
                   if x['name'] == "Resolve Issue"][0]

        # ARROW-6915: do not overwrite existing fix versions corresponding to
        # point releases
        fix_versions = list(fix_versions)
        fix_version_names = set(x['name'] for x in fix_versions)
        for version in self.current_fix_versions:
            major, minor, patch = version.name.split('.')
            if patch != '0' and version.name not in fix_version_names:
                fix_versions.append(version.raw)

        self.jira_con.transition_issue(self.jira_id, resolve["id"],
                                       comment=comment,
                                       fixVersions=fix_versions)

        print("Successfully resolved %s!" % (self.jira_id))

        self.issue = self.jira_con.issue(self.jira_id)
        self.show()

    def show(self):
        fields = self.issue.fields
        print(format_jira_output(self.jira_id, fields.status.name,
                                 fields.summary, fields.assignee,
                                 fields.components))


def format_jira_output(jira_id, status, summary, assignee, components):
    if assignee is None:
        assignee = "NOT ASSIGNED!!!"
    else:
        assignee = assignee.displayName

    if len(components) == 0:
        components = 'NO COMPONENTS!!!'
    else:
        components = ', '.join((x.name for x in components))

    return """=== JIRA {} ===
Summary\t\t{}
Assignee\t{}
Components\t{}
Status\t\t{}
URL\t\t{}/{}""".format(jira_id, summary, assignee, components, status,
                       '/'.join((JIRA_API_BASE, 'browse')), jira_id)


class GitHubAPI(object):

    def __init__(self, project_name, cmd):
        self.github_api = ("https://api.github.com/repos/apache/{0}"
                           .format(project_name))

        token = None
        config = load_configuration()
        if "github" in config.sections():
            token = config["github"]["api_token"]
        if not token:
            token = os.environ.get('ARROW_GITHUB_API_TOKEN')
        if not token:
            token = cmd.prompt('Env ARROW_GITHUB_API_TOKEN not set, '
                               'please enter your GitHub API token '
                               '(GitHub personal access token):')
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': 'token {0}'.format(token),
        }
        self.headers = headers

    def get_pr_data(self, number):
        return get_json("%s/pulls/%s" % (self.github_api, number),
                        headers=self.headers)

    def get_pr_commits(self, number):
        return get_json("%s/pulls/%s/commits" % (self.github_api, number),
                        headers=self.headers)

    def merge_pr(self, number, commit_title, commit_message):
        url = f'{self.github_api}/pulls/{number}/merge'
        payload = {
            'commit_title': commit_title,
            'commit_message': commit_message,
            'merge_method': 'squash',
        }
        response = requests.put(url, headers=self.headers, json=payload)
        result = response.json()
        if response.status_code != 200 and 'merged' not in result:
            result['merged'] = False
            result['message'] += f': {url}'
        return result


class CommandInput(object):
    """
    Interface to input(...) to enable unit test mocks to be created
    """

    def fail(self, msg):
        raise Exception(msg)

    def prompt(self, prompt):
        return input(prompt)

    def getpass(self, prompt):
        return getpass.getpass(prompt)

    def continue_maybe(self, prompt):
        while True:
            result = input("\n%s (y/n): " % prompt)
            if result.lower() == "y":
                return
            elif result.lower() == "n":
                self.fail("Okay, exiting")
            else:
                prompt = "Please input 'y' or 'n'"


class PullRequest(object):

    def __init__(self, cmd, github_api, git_remote, jira_con, number):
        self.cmd = cmd
        self._github_api = github_api
        self.git_remote = git_remote
        self.con = jira_con
        self.number = number
        self._pr_data = github_api.get_pr_data(number)
        try:
            self.url = self._pr_data["url"]
            self.title = self._pr_data["title"]
            self.body = self._pr_data["body"]
            self.target_ref = self._pr_data["base"]["ref"]
            self.user_login = self._pr_data["user"]["login"]
            self.base_ref = self._pr_data["head"]["ref"]
        except KeyError:
            pprint.pprint(self._pr_data)
            raise
        self.description = "%s/%s" % (self.user_login, self.base_ref)

        self.jira_issue = self._get_jira()

    def show(self):
        print("\n=== Pull Request #%s ===" % self.number)
        print("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s"
              % (self.title, self.description, self.target_ref, self.url))
        if self.jira_issue is not None:
            self.jira_issue.show()
        else:
            print("Minor PR.  Please ensure it meets guidelines for minor.\n")

    @property
    def is_merged(self):
        return bool(self._pr_data["merged"])

    @property
    def is_mergeable(self):
        return bool(self._pr_data["mergeable"])

    def _get_jira(self):
        if self.title.startswith("MINOR:"):
            return None

        jira_id = None
        for project, regex in PR_TITLE_REGEXEN:
            m = regex.search(self.title)
            if m:
                jira_id = m.group(1)
                break

        if jira_id is None:
            options = ' or '.join('{0}-XXX'.format(project)
                                  for project in SUPPORTED_PROJECTS)
            self.cmd.fail("PR title should be prefixed by a jira id "
                          "{0}, but found {1}".format(options, self.title))

        return JiraIssue(self.con, jira_id, project, self.cmd)

    def merge(self):
        """
        merge the requested PR and return the merge hash
        """
        commits = self._github_api.get_pr_commits(self.number)

        def format_commit_author(commit):
            author = commit['commit']['author']
            name = author['name']
            email = author['email']
            return f'{name} <{email}>'
        commit_authors = [format_commit_author(commit) for commit in commits]
        co_authored_by_re = re.compile(r'^Co-authored-by:\s*(.*)')

        def extract_co_authors(commit):
            message = commit['commit']['message']
            return co_authored_by_re.findall(message)
        commit_co_authors = []
        for commit in commits:
            commit_co_authors.extend(extract_co_authors(commit))

        all_commit_authors = commit_authors + commit_co_authors
        distinct_authors = sorted(set(all_commit_authors),
                                  key=lambda x: commit_authors.count(x),
                                  reverse=True)

        for i, author in enumerate(distinct_authors):
            print("Author {}: {}".format(i + 1, author))

        if len(distinct_authors) > 1:
            primary_author, distinct_other_authors = get_primary_author(
                self.cmd, distinct_authors)
        else:
            # If there is only one author, do not prompt for a lead author
            primary_author = distinct_authors.pop()
            distinct_other_authors = []

        commit_title = f'{self.title} (#{self.number})'
        commit_message_chunks = []
        if self.body is not None:
            commit_message_chunks.append(self.body)

        committer_name = run_cmd("git config --get user.name").strip()
        committer_email = run_cmd("git config --get user.email").strip()

        authors = ("Authored-by:" if len(distinct_other_authors) == 0
                   else "Lead-authored-by:")
        authors += " %s" % primary_author
        if len(distinct_authors) > 0:
            authors += "\n" + "\n".join(["Co-authored-by: %s" % a
                                         for a in distinct_other_authors])
        authors += "\n" + "Signed-off-by: %s <%s>" % (committer_name,
                                                      committer_email)
        commit_message_chunks.append(authors)

        commit_message = "\n\n".join(commit_message_chunks)

        if DEBUG:
            print(commit_title)
            print()
            print(commit_message)

        if DEBUG:
            merge_hash = None
        else:
            result = self._github_api.merge_pr(self.number,
                                               commit_title,
                                               commit_message)
            if not result['merged']:
                message = result['message']
                self.cmd.fail(f'Failed to merge pull request: {message}')
            merge_hash = result['sha']

        print("Pull request #%s merged!" % self.number)
        print("Merge hash: %s" % merge_hash)


def get_primary_author(cmd, distinct_authors):
    author_pat = re.compile(r'(.*) <(.*)>')

    while True:
        primary_author = cmd.prompt(
            "Enter primary author in the format of "
            "\"name <email>\" [%s]: " % distinct_authors[0])

        if primary_author == "":
            return distinct_authors[0], distinct_authors[1:]

        if author_pat.match(primary_author):
            break
        print('Bad author "{}", please try again'.format(primary_author))

    # When primary author is specified manually, de-dup it from
    # author list and put it at the head of author list.
    distinct_other_authors = [x for x in distinct_authors
                              if x != primary_author]
    return primary_author, distinct_other_authors


def prompt_for_fix_version(cmd, jira_issue):
    (all_versions,
     default_fix_versions) = jira_issue.get_candidate_fix_versions()

    default_fix_versions = ",".join(default_fix_versions)

    issue_fix_versions = cmd.prompt("Enter comma-separated "
                                    "fix version(s) [%s]: "
                                    % default_fix_versions)
    if issue_fix_versions == "":
        issue_fix_versions = default_fix_versions
    issue_fix_versions = issue_fix_versions.replace(" ", "").split(",")

    def get_version_json(version_str):
        return [x for x in all_versions if x.name == version_str][0].raw

    return [get_version_json(v) for v in issue_fix_versions]


CONFIG_FILE = "~/.config/arrow/merge.conf"


def load_configuration():
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(CONFIG_FILE))
    return config


def get_credentials(cmd):
    token = None

    config = load_configuration()
    if "jira" in config.sections():
        token = config["jira"].get("token")

    # Fallback to environment variables
    if not token:
        token = os.environ.get("APACHE_JIRA_TOKEN")

    # Fallback to user tty prompt
    if not token:
        token = cmd.prompt("Env APACHE_JIRA_TOKEN not set, "
                           "please enter your Jira API token "
                           "(Jira personal access token):")

    return token


def connect_jira(cmd):
    return jira.client.JIRA(options={'server': JIRA_API_BASE},
                            token_auth=get_credentials(cmd))


def get_pr_num():
    if len(sys.argv) == 2:
        return sys.argv[1]

    return input("Which pull request would you like to merge? (e.g. 34): ")


def cli():
    # Location of your Arrow git clone
    ARROW_HOME = os.path.abspath(os.path.dirname(__file__))
    PROJECT_NAME = os.environ.get('ARROW_PROJECT_NAME') or 'arrow'
    print("ARROW_HOME = " + ARROW_HOME)
    print("PROJECT_NAME = " + PROJECT_NAME)

    cmd = CommandInput()

    pr_num = get_pr_num()

    os.chdir(ARROW_HOME)

    github_api = GitHubAPI(PROJECT_NAME, cmd)

    jira_con = connect_jira(cmd)
    pr = PullRequest(cmd, github_api, PR_REMOTE_NAME, jira_con, pr_num)

    if pr.is_merged:
        print("Pull request %s has already been merged" % pr_num)
        sys.exit(0)

    if not pr.is_mergeable:
        print("Pull request %s is not mergeable in its current form" % pr_num)
        sys.exit(1)

    pr.show()

    cmd.continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    pr.merge()

    if pr.jira_issue is None:
        print("Minor PR.  No JIRA issue to update.\n")
        return

    cmd.continue_maybe("Would you like to update the associated JIRA?")
    jira_comment = (
        "Issue resolved by pull request %s\n[%s/%s]"
        % (pr_num,
           "https://github.com/apache/" + PROJECT_NAME + "/pull",
           pr_num))

    fix_versions_json = prompt_for_fix_version(cmd, pr.jira_issue)
    pr.jira_issue.resolve(fix_versions_json, jira_comment)


if __name__ == '__main__':
    try:
        cli()
    except Exception:
        raise
