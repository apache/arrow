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
#   - GH_TOKEN: a GitHub API token to use for API requests
#   - ARROW_GITHUB_API_TOKEN: Same as GH_TOKEN. For backward compatibility.
#   - ARROW_GITHUB_ORG: the GitHub organisation ('apache' by default)
#   - DEBUG: use for testing to avoid pushing to apache (0 by default)

import configparser
import os
import pprint
import re
import subprocess
import sys
import requests
import getpass

# Remote name which points to the GitHub site
ORG_NAME = (
    os.environ.get("ARROW_GITHUB_ORG") or
    os.environ.get("PR_REMOTE_NAME") or  # backward compatibility
    "apache"
)
PROJECT_NAME = os.environ.get('ARROW_PROJECT_NAME') or "arrow"

# For testing to avoid accidentally pushing to apache
DEBUG = bool(int(os.environ.get("DEBUG", 0)))

if DEBUG:
    print("**************** DEBUGGING ****************")


def get_json(url, headers=None):
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError(response.json())
    # GitHub returns a link header with the next, previous, last
    # page if there is pagination on the response. See:
    # https://docs.github.com/en/rest/guides/using-pagination-in-the-rest-api#using-link-headers
    next_responses = None
    if "link" in response.headers:
        links = response.headers['link'].split(', ')
        for link in links:
            if 'rel="next"' in link:
                # Format: '<url>; rel="next"'
                next_url = link.split(";")[0][1:-1]
                next_responses = get_json(next_url, headers)
    responses = response.json()
    if next_responses:
        if isinstance(responses, list):
            responses.extend(next_responses)
        else:
            raise ValueError('GitHub response was paginated and is not a list')
    return responses


def run_cmd(cmd):
    if isinstance(cmd, str):
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

    if isinstance(output, bytes):
        output = output.decode('utf-8')
    return output


_REGEX_CI_DIRECTIVE = re.compile(r'\[[^\]]*\]')


def strip_ci_directives(commit_message):
    # Remove things like '[force ci]', '[skip appveyor]' from the assembled
    # commit message
    return _REGEX_CI_DIRECTIVE.sub('', commit_message)


def fix_version_from_branch(versions):
    # Note: Assumes this is a sorted (newest->oldest) list of un-released
    # versions
    return versions[-1]


MIGRATION_COMMENT_REGEX = re.compile(
    r"This issue has been migrated to \[issue #(?P<issue_id>(\d+))"
)


class GitHubIssue(object):

    def __init__(self, github_api, github_id, cmd):
        self.github_api = github_api
        self.github_id = github_id
        self.cmd = cmd

        try:
            self.issue = self.github_api.get_issue_data(github_id)
        except Exception as e:
            self.cmd.fail("GitHub could not find %s\n%s" % (github_id, e))

    def get_label(self, prefix):
        prefix = f"{prefix}:"
        return [
            lbl["name"][len(prefix):].strip()
            for lbl in self.issue["labels"] if lbl["name"].startswith(prefix)
        ]

    @property
    def components(self):
        return self.get_label("Component")

    @property
    def assignees(self):
        return [a["login"] for a in self.issue["assignees"]]

    @property
    def current_fix_versions(self):
        try:
            return self.issue.get("milestone", {}).get("title")
        except AttributeError:
            pass

    @property
    def current_versions(self):
        all_versions = self.github_api.get_milestones()

        unreleased_versions = [x for x in all_versions if x["state"] == "open"]
        unreleased_versions = [x["title"] for x in unreleased_versions]

        return unreleased_versions

    def resolve(self, fix_version, comment, pr_body):
        cur_status = self.issue["state"]

        if cur_status == "closed":
            self.cmd.fail("GitHub issue %s already has status '%s'"
                          % (self.github_id, cur_status))

        if DEBUG:
            print("GitHub issue %s untouched -> %s" %
                  (self.github_id, fix_version))
        else:
            self.github_api.assign_milestone(self.github_id, fix_version)
            if f"Closes: #{self.github_id}" not in pr_body:
                self.github_api.close_issue(self.github_id, comment)
            print("Successfully resolved %s!" % (self.github_id))

        self.issue = self.github_api.get_issue_data(self.github_id)
        self.show()

    def show(self):
        issue = self.issue
        print(format_issue_output("github", self.github_id, issue["state"],
                                  issue["title"], ', '.join(self.assignees),
                                  self.components))


def get_candidate_fix_version(mainline_versions,
                              maintenance_branches=()):

    all_versions = [getattr(v, "name", v) for v in mainline_versions]

    def version_tuple(x):
        # Parquet versions are something like cpp-1.2.0
        numeric_version = getattr(x, "name", x).split("-", 1)[-1]
        return tuple(int(_) for _ in numeric_version.split("."))
    all_versions = sorted(all_versions, key=version_tuple, reverse=True)

    # Only suggest versions starting with a number, like 0.x but not JS-0.x
    mainline_versions = all_versions
    major_versions = [v for v in mainline_versions if v.endswith('.0.0')]

    if len(mainline_versions) > len(major_versions):
        # If there is a future major release, suggest that
        mainline_versions = major_versions

    mainline_versions = [v for v in mainline_versions
                         if f"maint-{v}" not in maintenance_branches]
    default_fix_versions = fix_version_from_branch(mainline_versions)

    return default_fix_versions


def format_issue_output(issue_type, issue_id, status,
                        summary, assignee, components):
    if not assignee:
        assignee = "NOT ASSIGNED!!!"
    else:
        assignee = getattr(assignee, "displayName", assignee)

    if len(components) == 0:
        components = 'NO COMPONENTS!!!'
    else:
        components = ', '.join((getattr(x, "name", x) for x in components))

    url_id = issue_id
    if "GH" in issue_id:
        url_id = issue_id.replace("GH-", "")

    url = f'https://github.com/{ORG_NAME}/{PROJECT_NAME}/issues/{url_id}'

    return f"""=== {issue_type.upper()} {issue_id} ===
Summary\t\t{summary}
Assignee\t{assignee}
Components\t{components}
Status\t\t{status}
URL\t\t{url}"""


class GitHubAPI(object):

    def __init__(self, project_name, cmd):
        self.github_api = (
            f"https://api.github.com/repos/{ORG_NAME}/{project_name}"
        )

        token = None
        config = load_configuration()
        if "github" in config.sections():
            token = config["github"]["api_token"]
        if not token:
            token = os.environ.get('GH_TOKEN')
        if not token:
            token = os.environ.get('ARROW_GITHUB_API_TOKEN')
            if token:
                print('ARROW_GITHUB_API_TOKEN environment variable is '
                      'deprecated. Use GH_TOKEN environment variable instead.')
        if not token:
            token = cmd.prompt('Env GH_TOKEN nor '
                               'ARROW_GITHUB_API_TOKEN not set, '
                               'please enter your GitHub API token '
                               '(GitHub personal access token):')
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': f'token {token}',
        }
        self.headers = headers

    def get_milestones(self):
        return get_json("%s/milestones" % (self.github_api, ),
                        headers=self.headers)

    def get_milestone_number(self, version):
        return next((
            m["number"] for m in self.get_milestones() if m["title"] == version
        ), None)

    def get_issue_data(self, number):
        return get_json("%s/issues/%s" % (self.github_api, number),
                        headers=self.headers)

    def get_pr_data(self, number):
        return get_json("%s/pulls/%s" % (self.github_api, number),
                        headers=self.headers)

    def get_pr_commits(self, number):
        return get_json("%s/pulls/%s/commits" % (self.github_api, number),
                        headers=self.headers)

    def get_branches(self):
        return get_json("%s/branches" % (self.github_api),
                        headers=self.headers)

    def close_issue(self, number, comment):
        issue_url = f'{self.github_api}/issues/{number}'
        comment_url = f'{self.github_api}/issues/{number}/comments'

        r = requests.post(comment_url, json={
                          "body": comment}, headers=self.headers)
        if not r.ok:
            raise ValueError(
                f"Failed request: {comment_url}:{r.status_code} -> {r.json()}")

        r = requests.patch(
            issue_url, json={"state": "closed"}, headers=self.headers)
        if not r.ok:
            raise ValueError(
                f"Failed request: {issue_url}:{r.status_code} -> {r.json()}")

    def assign_milestone(self, number, version):
        url = f'{self.github_api}/issues/{number}'
        milestone_number = self.get_milestone_number(version)
        if not milestone_number:
            raise ValueError(f"Invalid version {version}, milestone not found")
        payload = {
            'milestone': milestone_number
        }
        r = requests.patch(url, headers=self.headers, json=payload)
        if not r.ok:
            raise ValueError(
                f"Failed request: {url}:{r.status_code} -> {r.json()}")
        return r.json()

    def merge_pr(self, number, commit_title, commit_message):
        url = f'{self.github_api}/pulls/{number}/merge'
        payload = {
            'commit_title': commit_title,
            'commit_message': commit_message,
            'merge_method': 'squash',
        }
        response = requests.put(url, headers=self.headers, json=payload)
        result = response.json()
        if response.status_code == 200 and 'merged' in result:
            self.clear_pr_state_labels(number)
        else:
            result['merged'] = False
            result['message'] += f': {url}'
        return result

    def clear_pr_state_labels(self, number):
        url = f'{self.github_api}/issues/{number}/labels'
        response = requests.get(url, headers=self.headers)
        labels = response.json()
        for label in labels:
            # All PR workflow state labels starts with "awaiting"
            if label['name'].startswith('awaiting'):
                label_url = f"{url}/{label['name']}"
                requests.delete(label_url, headers=self.headers)


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
    GITHUB_PR_TITLE_PATTERN = re.compile(r'^GH-([0-9]+)\b.*$')

    def __init__(self, cmd, github_api, git_remote, number):
        self.cmd = cmd
        self._github_api = github_api
        self.git_remote = git_remote
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

        self.issue = self._get_issue()

    def show(self):
        print("\n=== Pull Request #%s ===" % self.number)
        print("title\t%s\nsource\t%s\ntarget\t%s\nurl\t%s"
              % (self.title, self.description, self.target_ref, self.url))
        if self.issue is not None:
            self.issue.show()
        else:
            print("Minor PR.  Please ensure it meets guidelines for minor.\n")

    @property
    def is_merged(self):
        return bool(self._pr_data["merged"])

    @property
    def is_mergeable(self):
        return bool(self._pr_data["mergeable"])

    @property
    def maintenance_branches(self):
        return [x["name"] for x in self._github_api.get_branches()
                if x["name"].startswith("maint-")]

    def _get_issue(self):
        if self.title.startswith("MINOR:"):
            return None

        m = self.GITHUB_PR_TITLE_PATTERN.search(self.title)
        if m:
            github_id = m.group(1)
            return GitHubIssue(self._github_api, github_id, self.cmd)

        self.cmd.fail("PR title should be prefixed by a GitHub ID, like: "
                      f"GH-XXX, but found {self.title}")

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
        co_authored_by_re = re.compile(
            r'^Co-authored-by:\s*(.*)', re.MULTILINE)

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
            print(f"Author {i + 1}: {author}")

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
            # Remove comments (i.e. <-- comment -->) from the PR description.
            body = re.sub(r"<!--.*?-->", "", self.body, flags=re.DOTALL)
            # avoid github user name references by inserting a space after @
            body = re.sub(r"@(\w+)", "@ \\1", body)
            commit_message_chunks.append(body)

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

        # Normalize line ends and collapse extraneous newlines. We allow two
        # consecutive newlines for paragraph breaks but not more.
        commit_message = "\n".join(commit_message.splitlines())
        commit_message = re.sub("\n{2,}", "\n\n", commit_message)

        if DEBUG:
            print("*** Commit title ***")
            print(commit_title)
            print()
            print("*** Commit message ***")
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
        print(f'Bad author "{primary_author}", please try again')

    # When primary author is specified manually, de-dup it from
    # author list and put it at the head of author list.
    distinct_other_authors = [x for x in distinct_authors
                              if x != primary_author]
    return primary_author, distinct_other_authors


def prompt_for_fix_version(cmd, issue, maintenance_branches=()):
    default_fix_version = get_candidate_fix_version(
        mainline_versions=issue.current_versions,
        maintenance_branches=maintenance_branches
    )

    current_fix_versions = issue.current_fix_versions
    if (current_fix_versions and
            current_fix_versions != default_fix_version):
        print("\n=== The assigned milestone is not the default ===")
        print(f"Assigned milestone: {current_fix_versions}")
        print(f"Current milestone: {default_fix_version}")
        if issue.issue["milestone"].get("state") == 'closed':
            print("The assigned milestone state is closed. Contact the ")
            print("Release Manager if it has to be added to a closed Release")
        print("Please ensure to assign the correct milestone.")
        # Default to existing assigned milestone
        default_fix_version = current_fix_versions

    issue_fix_version = cmd.prompt("Enter fix version [%s]: "
                                   % default_fix_version)
    if issue_fix_version == "":
        issue_fix_version = default_fix_version
    issue_fix_version = issue_fix_version.strip()
    return issue_fix_version


CONFIG_FILE = "~/.config/arrow/merge.conf"


def load_configuration():
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(CONFIG_FILE))
    return config


def get_pr_num():
    if len(sys.argv) == 2:
        return sys.argv[1]

    return input("Which pull request would you like to merge? (e.g. 34): ")


def cli():
    # Location of your Arrow git clone
    ARROW_HOME = os.path.abspath(os.path.dirname(__file__))
    print(f"ARROW_HOME = {ARROW_HOME}")
    print(f"ORG_NAME = {ORG_NAME}")
    print(f"PROJECT_NAME = {PROJECT_NAME}")

    cmd = CommandInput()

    pr_num = get_pr_num()

    os.chdir(ARROW_HOME)

    github_api = GitHubAPI(PROJECT_NAME, cmd)
    pr = PullRequest(cmd, github_api, ORG_NAME, pr_num)

    if pr.is_merged:
        print("Pull request %s has already been merged" % pr_num)
        sys.exit(0)

    if not pr.is_mergeable:
        print("Pull request %s is not mergeable in its current form" % pr_num)
        sys.exit(1)

    pr.show()

    cmd.continue_maybe("Proceed with merging pull request #%s?" % pr_num)

    pr.merge()

    if pr.issue is None:
        print("Minor PR.  No issue to update.\n")
        return

    cmd.continue_maybe("Would you like to update the associated issue?")
    issue_comment = (
        "Issue resolved by pull request %s\n%s"
        % (pr_num,
           f"https://github.com/{ORG_NAME}/{PROJECT_NAME}/pull/{pr_num}")
    )
    fix_version = prompt_for_fix_version(cmd, pr.issue,
                                         pr.maintenance_branches)
    pr.issue.resolve(fix_version, issue_comment, pr.body)


if __name__ == '__main__':
    try:
        cli()
    except Exception:
        raise
