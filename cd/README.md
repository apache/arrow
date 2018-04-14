# Arrow Packaging

Currently Arrow ships the following pre-built binaries:
- C++ and Python [conda-forge packages](conda-recipes) for Linux, Mac and
  Windows
- Python [Wheels](python-wheels) for Linux, Mac and Windows
- C++ GLib [Linux packages](linux-packages) for multiple distros

The content of this directory aims for automatizing the historically error prone
process of Arrow packaging as well as running nightly builds.


## Architecture

### Executors

Individual builds are executed on public CI services, currently:
- Linux: TravisCI
- Mac: TravisCI
- Windows: AppVeyor

### Queue

Because of the nature of how the CI services work, the scheduling of happens
through an additional git repository, which acts like job queue for the builds.
A job is a git commit on a particular git branch, containing only the required
configuration file to run the CI build.

### Scheduler

[Crossbow.py](crossbow.py) handles the versioning, configuration rendering, git
commit creation and pushing the changes.


## Install

> The following guide depends on GitHub, but theoretically any git server can be
> used.

1. [Create the queue
   repository](https://help.github.com/articles/creating-a-new-repository)
2. Enable [TravisCI](https://travis-ci.org/getting_started) and
   [Appveyor](https://www.appveyor.com/docs/) integrations on it
3. Clone the newly created, by default the scripts looks for `crossbow` next to
   arrow repository.

   ```bash
   git clone https://github.com/<user>/crossbow crossbow
   ```

4. [Create a Personal Access
   Token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/)
5. Export the token as an environment variable:

   ```bash
   export CROSSBOW_GITHUB_TOKEN=<token>
   ```

   > or pass as an argument to the CLI script `--github-token`

6. Install the python dependencies for the script:

   ```bash
   conda install -y jinja2 pygit2 click
   ```
7. Try running it:
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

   $ cd arrow/cd
   $ python crossbow.py
   ```

2. Gets the HEAD commit of the currently checked out branch and generates
   the version number based on [setuptools_scm](https://pypi.python.org/pypi/setuptools_scm).
   So to build a particular branch, just check out before running the script:

   ```bash
   git checkout ARROW-<ticket number>
   python cd/crossbow.py --dry-run
   ```

   > Note that the arrow branch must be pushed beforehand, because the script
   > will clone the selected branch.

3. Reads and renders the required build configurations with the parameters
   substituted.
2. Create a commit per build configuration to its own branch. For example
   to build `travis-linux-conda.yml` it will place a commit to the tip of
   `crossbow@travis-linux-conda` branch.
3. Pushes the modified branches to GitHub which triggers the builds.
   For authentication it uses github oauth tokens described in the install
   section.


### Examples

The script accepts a pattern as a first argument to narrow the build scope:

Run all builds:

```bash
$ python crossbow.py
Repository: https://github.com/kszucs/arrow@cd
Commit SHA: 810a718836bb3a8cefc053055600bdcc440e6702
Version: 0.9.1.dev48+g810a7188.d20180414
Pushed branches:
 - travis-osx-wheel
 - travis-linux-packages
 - travis-linux-wheel
 - appveyor-win-wheel
 - appveyor-win-conda
 - travis-linux-conda
 - travis-osx-conda
```

Just render without applying or committing the changes:

```bash
$ python crossbow.py --dry-run
```

Run only `conda` package builds but on all platforms:

```bash
$ python crossbow.py conda
Repository: https://github.com/kszucs/arrow@cd
Commit SHA: 810a718836bb3a8cefc053055600bdcc440e6702
Version: 0.9.1.dev48+g810a7188.d20180414
Pushed branches:
 - appveyor-win-conda
 - travis-linux-conda
 - travis-osx-conda
```

Run `wheel` builds:

```bash
$ python crossbow.py wheel
Repository: https://github.com/kszucs/arrow@cd
Commit SHA: 810a718836bb3a8cefc053055600bdcc440e6702
Version: 0.9.1.dev48+g810a7188.d20180414
Pushed branches:
 - travis-osx-wheel
 - travis-linux-wheel
 - appveyor-win-wheel
```

Run only `linux-conda` package build:

```bash
$ python crossbow.py linux-conda
Repository: https://github.com/kszucs/arrow@cd
Commit SHA: 810a718836bb3a8cefc053055600bdcc440e6702
Version: 0.9.1.dev48+g810a7188.d20180414
Pushed branches:
 - travis-linux-conda
```

