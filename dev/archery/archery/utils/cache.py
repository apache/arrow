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

from pathlib import Path
import os
from urllib.request import urlopen

from .logger import logger

ARCHERY_CACHE_DIR = Path.home() / ".cache" / "archery"


class Cache:
    """ Cache stores downloaded objects, notably apache-rat.jar. """

    def __init__(self, path=ARCHERY_CACHE_DIR):
        self.root = path

        if not path.exists():
            os.makedirs(path)

    def key_path(self, key):
        """ Return the full path of a key. """
        return self.root/key

    def get(self, key):
        """ Return the full path of a key if cached, None otherwise. """
        path = self.key_path(key)
        return path if path.exists() else None

    def delete(self, key):
        """ Remove a key (and the file) from the cache. """
        path = self.get(key)
        if path:
            path.unlink()

    def get_or_insert(self, key, create):
        """
        Get or Insert a key from the cache. If the key is not found, the
        `create` closure will be evaluated.

        The `create` closure takes a single parameter, the path where the
        object should be store. The file should only be created upon success.
        """
        path = self.key_path(key)

        if not path.exists():
            create(path)

        return path

    def get_or_insert_from_url(self, key, url):
        """
        Get or Insert a key from the cache. If the key is not found, the file
        is downloaded from `url`.
        """
        def download(path):
            """ Tiny wrapper that download a file and save as key. """
            logger.debug("Downloading {} as {}".format(url, path))
            conn = urlopen(url)
            # Ensure the download is completed before writing to disks.
            content = conn.read()
            with open(path, "wb") as path_fd:
                path_fd.write(content)

        return self.get_or_insert(key, download)
