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

"""Download release binaries."""

import argparse
import concurrent.futures as cf
import functools
import json
import os
import random
import re
import subprocess
import time
import urllib.request


DEFAULT_PARALLEL_DOWNLOADS = 8


class Downloader:

    def get_file_list(self, prefix, filter=None):
        def traverse(directory, files, directories):
            url = f'{self.URL_ROOT}/{directory}'
            response = urllib.request.urlopen(url).read().decode()
            paths = re.findall('<a href="(.+?)"', response)
            for path in paths:
                path = re.sub(f'^{re.escape(url)}',
                              '',
                              path)
                if path == '../':
                    continue
                resolved_path = f'{directory}{path}'
                if filter and not filter(path):
                    continue
                if path.endswith('/'):
                    directories.append(resolved_path)
                else:
                    files.append(resolved_path)
        files = []
        if prefix != '' and not prefix.endswith('/'):
            prefix += '/'
        directories = [prefix]
        while len(directories) > 0:
            directory = directories.pop()
            traverse(directory, files, directories)
        return files

    def download_files(self, files, dest=None, num_parallel=None,
                       re_match=None):
        """
        Download files from Bintray in parallel. If file already exists, will
        overwrite if the checksum does not match what Bintray says it should be

        Parameters
        ----------
        files : List[Dict]
            File listing from Bintray
        dest : str, default None
            Defaults to current working directory
        num_parallel : int, default 8
            Number of files to download in parallel. If set to None, uses
            default
        """
        if dest is None:
            dest = os.getcwd()
        if num_parallel is None:
            num_parallel = DEFAULT_PARALLEL_DOWNLOADS

        if re_match is not None:
            regex = re.compile(re_match)
            files = [x for x in files if regex.match(x)]

        if num_parallel == 1:
            for path in files:
                self._download_file(dest, path)
        else:
            parallel_map_terminate_early(
                functools.partial(self._download_file, dest),
                files,
                num_parallel
            )

    def _download_file(self, dest, path):
        base, filename = os.path.split(path)

        dest_dir = os.path.join(dest, base)
        os.makedirs(dest_dir, exist_ok=True)

        dest_path = os.path.join(dest_dir, filename)

        print("Downloading {} to {}".format(path, dest_path))

        url = f'{self.URL_ROOT}/{path}'
        self._download_url(url, dest_path)

    def _download_url(self, url, dest_path, *, extra_args=None):
        cmd = [
            "curl",
            "--fail",
            "--location",
            "--retry",
            "5",
            *(extra_args or []),
            "--output",
            dest_path,
            url,
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            try:
                # Don't leave possibly partial file around
                os.remove(dest_path)
            except IOError:
                pass
            raise Exception(f"Downloading {url} failed\n"
                            f"stdout: {stdout}\nstderr: {stderr}")

    def _curl_version(self):
        cmd = ["curl", "--version"]
        out = subprocess.run(cmd, capture_output=True, check=True).stdout
        match = re.search(r"curl (\d+)\.(\d+)\.(\d+) ", out.decode())
        return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


class Artifactory(Downloader):
    URL_ROOT = "https://apache.jfrog.io/artifactory/arrow"


class Maven(Downloader):
    URL_ROOT = "https://repository.apache.org" + \
        "/content/repositories/staging/org/apache/arrow"


class GitHub(Downloader):
    def __init__(self, repository, tag):
        super().__init__()
        if repository is None:
            raise ValueError("--repository is required")
        if tag is None:
            raise ValueError("--tag is required")
        self._repository = repository
        self._tag = tag

    def get_file_list(self, prefix, filter=None):
        url = (f"https://api.github.com/repos/{self._repository}/"
               f"releases/tags/{self._tag}")
        print("Fetching release from", url)
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Accept": "application/vnd.github+json",
            },
        )
        raw_response = urllib.request.urlopen(request).read().decode()
        response = json.loads(raw_response)

        files = []
        for asset in response["assets"]:
            if filter and not filter(asset["name"]):
                continue
            # Don't use the API URL since it has a fairly strict rate
            # limit unless logged in, and we have a lot of tiny
            # artifacts
            url = (
                f"https://github.com/{self._repository}/"
                f"releases/download/{self._tag}/{asset['name']}"
            )
            files.append((asset["name"], url))
        return files

    def _download_file(self, dest, asset):
        name, url = asset

        os.makedirs(dest, exist_ok=True)
        dest_path = os.path.join(dest, name)
        print(f"Downloading {url} to {dest_path}")

        if os.path.isfile(dest_path):
            print("Already downloaded", dest_path)
            return

        delay = random.randint(0, 3)
        print(f"Waiting {delay} seconds to avoid rate limit")
        time.sleep(delay)

        extra_args = [
            "--header",
            "Accept: application/octet-stream",
        ]
        if self._curl_version() >= (7, 71, 0):
            # Also retry 403s
            extra_args.append("--retry-all-errors")
        self._download_url(
            url,
            dest_path,
            extra_args=extra_args
        )


def parallel_map_terminate_early(f, iterable, num_parallel):
    tasks = []
    with cf.ProcessPoolExecutor(num_parallel) as pool:
        for v in iterable:
            tasks.append(pool.submit(functools.partial(f, v)))

        for task in cf.as_completed(tasks):
            if task.exception() is not None:
                e = task.exception()
                for task in tasks:
                    task.cancel()
                raise e


ARROW_REPOSITORY_PACKAGE_TYPES = [
    'almalinux',
    'amazon-linux',
    'centos',
    'debian',
    'ubuntu',
]
ARROW_STANDALONE_PACKAGE_TYPES = ['nuget', 'python']
ARROW_PACKAGE_TYPES = \
    ARROW_REPOSITORY_PACKAGE_TYPES + \
    ARROW_STANDALONE_PACKAGE_TYPES


def download_rc_binaries(version, rc_number, re_match=None, dest=None,
                         num_parallel=None, target_package_type=None,
                         repository=None, tag=None):
    version_string = '{}-rc{}'.format(version, rc_number)
    version_pattern = re.compile(r'\d+\.\d+\.\d+')
    if target_package_type:
        package_types = [target_package_type]
    else:
        package_types = ARROW_PACKAGE_TYPES
    for package_type in package_types:
        def is_target(path):
            match = version_pattern.search(path)
            if not match:
                return True
            return match[0] == version
        filter = is_target

        if package_type == 'jars':
            downloader = Maven()
            prefix = ''
        elif package_type == 'github':
            downloader = GitHub(repository, tag)
            prefix = ''
            filter = None
        elif package_type in ARROW_REPOSITORY_PACKAGE_TYPES:
            downloader = Artifactory()
            prefix = f'{package_type}-rc'
        else:
            downloader = Artifactory()
            prefix = f'{package_type}-rc/{version_string}'
            filter = None
        files = downloader.get_file_list(prefix, filter=filter)
        downloader.download_files(files, re_match=re_match, dest=dest,
                                  num_parallel=num_parallel)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Download release candidate binaries'
    )
    parser.add_argument('version', type=str, help='The version number')
    parser.add_argument('rc_number', type=int,
                        help='The release candidate number, e.g. 0, 1, etc')
    parser.add_argument('-e', '--regexp', type=str, default=None,
                        help=('Regular expression to match on file names '
                              'to only download certain files'))
    parser.add_argument('--dest', type=str, default=os.getcwd(),
                        help='The output folder for the downloaded files')
    parser.add_argument('--num_parallel', type=int,
                        default=DEFAULT_PARALLEL_DOWNLOADS,
                        help='The number of concurrent downloads to do')
    parser.add_argument('--package_type', type=str, default=None,
                        help='The package type to be downloaded')
    parser.add_argument('--repository', type=str,
                        help=('The repository to pull from '
                              '(required if --package_type=github)'))
    parser.add_argument('--tag', type=str,
                        help=('The release tag to download '
                              '(required if --package_type=github)'))
    args = parser.parse_args()

    download_rc_binaries(
        args.version,
        args.rc_number,
        dest=args.dest,
        re_match=args.regexp,
        num_parallel=args.num_parallel,
        target_package_type=args.package_type,
        repository=args.repository,
        tag=args.tag,
    )
