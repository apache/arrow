<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Arrow Packaging

The content of this directory aims for automating the historically error prone
process of Arrow packaging.

Packages:
- C++ and Python [conda-forge packages](conda-recipes) for Linux, Mac and
  Windows
- Python [Wheels](python-wheels) for Linux, Mac and Windows
- C++ and GLib [Linux packages](linux-packages) for multiple distributions
- Java

## Architecture

### Executors

Individual jobs are executed on public CI services, currently:
- Linux: TravisCI
- Mac: TravisCI
- Windows: AppVeyor

### Queue

Because of the nature of how the CI services work, the scheduling of jobs happens
through an additional git repository, which acts like a job queue for the tasks.
A job is a git commit on a particular git branch, containing only the required
configuration file to run the requested build (currently `.travis.yml` or
`appveyor.yml`).

### Scheduler

[Crossbow.py](crossbow.py) handles version generation, task rendering and
submission. The tasks are defined in `tasks.yml`


## Install

> The following guide depends on GitHub, but theoretically any git server can be
> used.

1. [Create the queue
   repository](https://help.github.com/articles/creating-a-new-repository)
2. Enable [TravisCI](https://travis-ci.org/getting_started) and
   [Appveyor](https://www.appveyor.com/docs/) integrations on it

   - turn off Travis' [auto cancellation](https://docs.travis-ci.com/user/customizing-the-build/#Building-only-the-latest-commit) feature on branches

3. Clone the newly created, by default the scripts looks for `crossbow` next to
   arrow repository.

   ```bash
   git clone https://github.com/<user>/crossbow crossbow
   ```

4. [Create a Personal Access
   Token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/)
5. Locally export the token as an environment variable:

   ```bash
   export CROSSBOW_GITHUB_TOKEN=<token>
   ```

   > or pass as an argument to the CLI script `--github-token`

6. Export the previously created GitHub token on both CI services:

   Use `CROSSBOW_GITHUB_TOKEN` encrypted environment variable. You can set them
   at the following URLs, where `ghuser` is the GitHub username and `ghrepo` is
   the GitHub repository name (typically `crossbow`):

   - TravisCI: `https://travis-ci.org/<ghuser>/<ghrepo>/settings`
   - Appveyor: `https://ci.appveyor.com/project/<ghuser>/<ghrepo>/settings/environment`

7. Install Python 3.6:

   Miniconda is preferred, see installation instructions:
   https://conda.io/docs/user-guide/install/index.html

8. Install the python dependencies for the script:

   ```bash
   conda install -c conda-forge -y jinja2 pygit2 click ruamel.yaml setuptools_scm github3.py python-gnupg toolz jira
   ```

   ```bash
   # pygit2 requires libgit2: http://www.pygit2.org/install.html
   pip install jinja2 pygit2 click ruamel.yaml setuptools_scm github3.py python-gnupg toolz jira
   ```

9. Try running it:
   ```bash
   $ python crossbow.py --help
   ```


## Usage

The script does the following:

1. Detects the current repository, thus supports forks. The following snippet
   will build kszucs's fork instead of the upstream apache/arrow repository.

   ```bash
   $ git clone https://github.com/kszucs/arrow
   $ git clone https://github.com/kszucs/crossbow

   $ cd arrow/dev/tasks
   $ python crossbow.py submit conda-win conda-linux conda-osx
   ```

2. Gets the HEAD commit of the currently checked out branch and generates
   the version number based on [setuptools_scm](https://pypi.python.org/pypi/setuptools_scm).
   So to build a particular branch, just check out before running the script:

   ```bash
   git checkout ARROW-<ticket number>
   python dev/tasks/crossbow.py submit --dry-run conda-linux conda-osx
   ```

   > Note that the arrow branch must be pushed beforehand, because the script
   > will clone the selected branch.

3. Reads and renders the required build configurations with the parameters
   substituted.
2. Create a branch per task, prefixed with the job id. For example
   to build conda recipes on linux it will create a new branch:
   `crossbow@build-<id>-conda-linux`.
3. Pushes the modified branches to GitHub which triggers the builds.
   For authentication it uses GitHub OAuth tokens described in the install
   section.


### Query the build status

```bash
python crossbow.py status <build id / branch name>
```

### Download the build artifacts

```bash
python crossbow.py artifacts <build id / branch name>
```

### Examples

The script accepts a pattern as a first argument to narrow the build scope:

Run multiple builds:

```bash
$ python crossbow.py submit debian-stretch conda-linux-py36 wheel-win-py36
Repository: https://github.com/kszucs/arrow@tasks
Commit SHA: 810a718836bb3a8cefc053055600bdcc440e6702
Version: 0.9.1.dev48+g810a7188.d20180414
Pushed branches:
 - debian-stretch
 - conda-linux-py36
 - wheel-win-py36
```

Just render without applying or committing the changes:

```bash
$ python crossbow.py submit --dry-run task_name
```

Run only `conda` package builds and a Linux one:

```bash
$ python crossbow.py submit -g conda centos-7
```

Run `wheel` builds:

```bash
$ python crossbow.py submit --group wheel
```
