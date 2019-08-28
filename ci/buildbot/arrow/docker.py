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

from ursabot.docker import ImageCollection, DockerImage, worker_image_for
from ursabot.docker import ENTRYPOINT, ADD, RUN, ENV, SHELL
from ursabot.docker import pip, apt, apk, symlink, conda
from ursabot.utils import Platform, read_dependency_list

"""
There is a small docker utility in ursabot.docker module to define
hierachical images. It uses a DSL implemented in python instead of plain
Dockerfiles. Additionally it plays nicely with the DockerLatentWorker and
the DockerBuilder abstractions. The DockerBuilder can specify which
DockerImage(s) it requires to run the build(s).
For more see https://github.com/ursa-labs/ursabot#define-docker-images
"""

# configure shell and entrypoint to load .bashrc
docker_assets = Path(__file__).parent.parent / 'docker'
python_symlinks = {'/usr/local/bin/python': '/usr/bin/python3',
                   '/usr/local/bin/pip': '/usr/bin/pip3'}

ubuntu_pkgs = read_dependency_list(docker_assets / 'pkgs-ubuntu.txt')
alpine_pkgs = read_dependency_list(docker_assets / 'pkgs-alpine.txt')
python_steps = [
    ADD(docker_assets / 'requirements.txt'),
    ADD(docker_assets / 'requirements-test.txt'),
    RUN(pip('cython', files=['requirements.txt'])),
    RUN(pip(files=['requirements-test.txt']))
]

# Note the python has a special treatment, because buildbot requires it.
# So all of the following images must have a python interpreter and pip
# pre-installed.
images = ImageCollection()


for arch in ['amd64', 'arm64v8', 'arm32v7']:
    # UBUNTU
    for ubuntu_version in ['18.04']:
        basetitle = f'{arch.upper()} Ubuntu {ubuntu_version}'

        cpp = DockerImage(
            name='cpp',
            title=f'{basetitle} C++',
            base=f'{arch}/ubuntu:{ubuntu_version}',
            org='ursalab',
            platform=Platform(
                arch=arch,
                system='linux',
                distro='ubuntu',
                version=ubuntu_version
            ),
            steps=[
                RUN(apt(*ubuntu_pkgs, 'ccache', 'python3', 'python3-pip')),
                RUN(symlink(python_symlinks))
            ]
        )
        cpp_benchmark = DockerImage(
            name='cpp-benchmark',
            base=cpp,
            title=f'{basetitle} C++ Benchmark',
            steps=[
                RUN(apt('libbenchmark-dev')),
                RUN(pip('click', 'pandas'))
            ]
        )
        r = DockerImage(
            name='r',
            base=cpp,
            title=f'{basetitle} R',
            steps=[
                ADD(docker_assets / 'install_r.sh'),
                RUN('/install_r.sh'),
                RUN(apt('libxml2-dev', 'libcurl4-openssl-dev')),
                ADD(docker_assets / 'install_r_deps.R'),
                RUN('/install_r_deps.R'),
            ]
        )
        python = DockerImage(
            name='python-3',
            base=cpp,
            title=f'{basetitle} Python 3',
            steps=python_steps
        )
        images.extend([cpp, cpp_benchmark, r, python])

    # ALPINE
    for alpine_version in ['3.9']:
        basetitle = f'{arch.upper()} Alpine {alpine_version}'

        cpp = DockerImage(
            name='cpp',
            title=f'{basetitle} C++',
            org='ursalab',
            base=f'{arch}/alpine:{alpine_version}',
            platform=Platform(
                arch=arch,
                system='linux',
                distro='alpine',
                version=alpine_version
            ),
            steps=[
                RUN(apk(*alpine_pkgs, 'ccache', 'python3-dev', 'py3-pip')),
                RUN(symlink(python_symlinks))
            ]
        )
        python = DockerImage(
            name='python-3',
            base=cpp,
            title=f'{basetitle} Python 3',
            steps=python_steps
        )
        images.extend([cpp, python])

# CONDA
for arch in ['amd64']:
    basetitle = f'{arch.upper()} Conda'
    miniconda_version = 'latest'

    base = DockerImage(
        name=f'base',
        title=basetitle,
        org='ursalab',
        base=f'{arch}/ubuntu:18.04',
        platform=Platform(
            arch=arch,
            system='linux',
            distro='ubuntu',
            version='18.04'
        ),
        variant='conda',
        steps=[
            RUN(apt('wget')),
            # install miniconda
            ENV(PATH='/opt/conda/bin:$PATH'),
            ADD(docker_assets / 'install_conda.sh'),
            RUN('/install_conda.sh', miniconda_version, arch, '/opt/conda'),
            # run conda activate
            SHELL(['/bin/bash', '-l', '-c']),
            ENTRYPOINT(['/bin/bash', '-l', '-c']),
        ]
    )

    crossbow = DockerImage(
        name='crossbow',
        base=base,
        title=f'{basetitle} Crossbow',
        steps=[
            # install crossbow dependencies
            ADD(docker_assets / 'conda-crossbow.txt'),
            RUN(conda('git', 'twisted', files=['conda-crossbow.txt'])),
        ]
    )
    cpp = DockerImage(
        name='cpp',
        base=base,
        title=f'{basetitle} C++',
        steps=[
            # install tzdata required for gandiva tests
            RUN(apt('tzdata')),
            # install cpp dependencies
            ADD(docker_assets / 'conda-linux.txt'),
            ADD(docker_assets / 'conda-cpp.txt'),
            RUN(conda('ccache', files=['conda-linux.txt', 'conda-cpp.txt'])),
        ]
    )
    cpp_benchmark = DockerImage(
        name='cpp-benchmark',
        base=cpp,
        title=f'{basetitle} C++ Benchmark',
        steps=[
            RUN(conda('benchmark', 'click', 'pandas'))
        ]
    )
    r = DockerImage(
        name='r',
        base=cpp,
        title=f'{basetitle} R',
        steps=[
            # install R dependencies
            ADD(docker_assets / 'conda-r.txt'),
            RUN(conda(files=['conda-r.txt']))
        ]
    )
    images.extend([crossbow, cpp, cpp_benchmark, r])

    for python_version in ['2.7', '3.6', '3.7']:
        python = DockerImage(
            name=f'python-{python_version}',
            base=cpp,
            title=f'{basetitle} Python {python_version}',
            steps=[
                ADD(docker_assets / 'conda-python.txt'),
                RUN(conda(f'python={python_version}',
                          files=['conda-python.txt']))
            ]
        )
        images.append(python)

# CUDA
for arch in ['amd64']:
    for toolkit_version in ['10.0']:
        basetitle = f'{arch.upper()} Nvidia Cuda {toolkit_version}'

        cpp = DockerImage(
            name='cpp',
            title=f'{basetitle} C++',
            org='ursalab',
            base=f'nvidia/cuda:{toolkit_version}-devel-ubuntu18.04',
            platform=Platform(
                arch=arch,
                system='linux',
                distro='ubuntu',
                version='18.04'
            ),
            variant='cuda',
            steps=[
                RUN(apt(*ubuntu_pkgs, 'ccache', 'python3', 'python3-pip')),
                RUN(symlink(python_symlinks))
            ]
        )
        python = DockerImage(
            name='python-3',
            base=cpp,
            title=f'{basetitle} Python 3',
            steps=python_steps
        )
        images.extend([cpp, python])

# JAVA
for arch in ['amd64']:
    maven_version = 3

    for java_version in ['8', '11']:
        java = DockerImage(
            name=f'java-{java_version}',
            title=f'{arch.upper()} Java OpenJDK {java_version}',
            org='ursalab',
            base=f'{arch}/maven:{maven_version}-jdk-{java_version}',
            platform=Platform(
                arch=arch,
                system='linux',
                distro='debian',
                version='9'
            ),
            steps=[
                RUN(apt('python3', 'python3-pip')),
                RUN(symlink(python_symlinks))
            ]
        )
        images.append(java)

# JAVASCRIPT
for arch in ['amd64']:
    for nodejs_version in ['11']:
        js = DockerImage(
            name=f'js-{nodejs_version}',
            title=f'{arch.upper()} Debian 9 NodeJS {nodejs_version}',
            org='ursalab',
            base=f'{arch}/node:{nodejs_version}-stretch',
            platform=Platform(
                arch=arch,
                system='linux',
                distro='debian',
                version='9'
            ),
            steps=[
                RUN(apt('python3', 'python3-pip')),
                RUN(symlink(python_symlinks))
            ]
        )
        images.append(js)

# GO
for arch in ['amd64']:
    for go_version in ['1.12.6', '1.11.11']:
        go = DockerImage(
            name=f'go-{go_version}',
            title=f'{arch.upper()} Debian 9 Go {go_version}',
            org='ursalab',
            base=f'{arch}/golang:{go_version}-stretch',
            platform=Platform(
                arch=arch,
                system='linux',
                distro='debian',
                version='9'
            ),
            steps=[
                RUN(apt('python3', 'python3-pip')),
                RUN(symlink(python_symlinks))
            ]
        )
        images.append(go)

# RUST
for arch in ['amd64']:
    for rust_version in ['1.35']:
        rust = DockerImage(
            name=f'rust-{rust_version}',
            title=f'{arch.upper()} Debian 9 Rust {rust_version}',
            org='ursalab',
            base=f'{arch}/rust:{rust_version}-stretch',
            platform=Platform(
                arch=arch,
                system='linux',
                distro='debian',
                version='9'
            ),
            steps=[
                RUN(apt('python3', 'python3-pip')),
                RUN(symlink(python_symlinks))
            ]
        )
        images.append(rust)

# WORKERS
# create worker containers for all of the images above, meaning installing
# buildbow-worker python package and the service configurations
images += [worker_image_for(image) for image in images]
