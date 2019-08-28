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

import textwrap

from buildbot.plugins import util
from ursabot.builders import DockerBuilder
from ursabot.steps import (SetPropertiesFromEnv, SetPropertyFromCommand,
                           Ninja, SetupPy, CTest, CMake, PyTest, Mkdir, Pip,
                           GitHub, Maven, Go, Cargo, Npm, R)
from ursabot.utils import Filter, Matching, AnyOf, Has, Extend, Merge

from .steps import Archery, Crossbow


# prefer GitHub over Git step
checkout_arrow = GitHub(
    name='Clone Arrow',
    repourl=util.Property('repository'),
    workdir='.',
    submodules=True,
    mode='full'
)

# explicitly define build definitions, exported via cmake -LAH
definitions = dict(
    # CMake flags
    CMAKE_BUILD_TYPE='debug',
    CMAKE_INSTALL_PREFIX=None,
    CMAKE_INSTALL_LIBDIR=None,
    CMAKE_CXX_FLAGS=None,
    CMAKE_AR=None,
    CMAKE_RANLIB=None,
    PYTHON_EXECUTABLE=None,
    # Build Arrow with Altivec
    ARROW_ALTIVEC='ON',
    # Rely on boost shared libraries where relevant
    ARROW_BOOST_USE_SHARED='ON',
    # Build the Arrow micro benchmarks
    ARROW_BUILD_BENCHMARKS='OFF',
    # Build the Arrow examples
    ARROW_BUILD_EXAMPLES='OFF',
    # Build shared libraries
    ARROW_BUILD_SHARED='ON',
    # Build static libraries
    ARROW_BUILD_STATIC='ON',
    # Build the Arrow googletest unit tests
    ARROW_BUILD_TESTS='ON',
    # Build Arrow commandline utilities
    ARROW_BUILD_UTILITIES='ON',
    # Build the Arrow Compute Modules
    ARROW_COMPUTE='ON',
    # Build the Arrow CUDA extensions (requires CUDA toolkit)
    ARROW_CUDA='OFF',
    # Compiler flags to append when compiling Arrow
    ARROW_CXXFLAGS='',
    # Compile with extra error context (line numbers, code)
    ARROW_EXTRA_ERROR_CONTEXT='ON',
    # Build the Arrow Flight RPC System (requires GRPC, Protocol Buffers)
    ARROW_FLIGHT='OFF',
    # Build Arrow Fuzzing executables
    # 'ARROW_FUZZING': 'OFF',
    # Build the Gandiva libraries
    ARROW_GANDIVA='OFF',
    # Build the Gandiva JNI wrappers
    ARROW_GANDIVA_JAVA='OFF',
    # Compiler flags to append when pre-compiling Gandiva operations
    ARROW_GANDIVA_PC_CXX_FLAGS=None,
    # Include -static-libstdc++ -static-libgcc when linking with Gandiva
    # static libraries
    # 'ARROW_GANDIVA_STATIC_LIBSTDCPP': 'OFF',
    # Build with C++ code coverage enabled
    # 'ARROW_GENERATE_COVERAGE': 'OFF',
    # Rely on GFlags shared libraries where relevant
    # 'ARROW_GFLAGS_USE_SHARED': 'ON',
    # Pass -ggdb flag to debug builds
    # 'ARROW_GGDB_DEBUG': 'ON',
    # Build the Arrow HDFS bridge
    ARROW_HDFS='OFF',
    # Build the HiveServer2 client and Arrow adapter
    # 'ARROW_HIVESERVER2': 'OFF',
    # Build Arrow libraries with install_name set to @rpath
    # 'ARROW_INSTALL_NAME_RPATH': 'ON',
    # Build the Arrow IPC extensions
    ARROW_IPC='ON',
    # Build the Arrow jemalloc-based allocator
    ARROW_JEMALLOC='ON',
    # Exclude deprecated APIs from build
    # 'ARROW_NO_DEPRECATED_API': 'OFF',
    # Only define the lint and check-format targets
    # 'ARROW_ONLY_LINT': 'OFF',
    # If enabled install ONLY targets that have already been built.
    # Please be advised that if this is enabled 'install' will fail silently
    # on components that have not been built.
    # 'ARROW_OPTIONAL_INSTALL': 'OFF',
    # Build the Arrow ORC adapter
    ARROW_ORC='OFF',
    # Build the Parquet libraries
    ARROW_PARQUET='OFF',
    # Build the plasma object store along with Arrow
    ARROW_PLASMA='OFF',
    # Build the plasma object store java client
    ARROW_PLASMA_JAVA_CLIENT='OFF',
    # Rely on Protocol Buffers shared libraries where relevant
    ARROW_PROTOBUF_USE_SHARED='ON',
    # Build the Arrow CPython extensions
    ARROW_PYTHON='OFF',
    # How to link the re2 library. static|shared
    # 'ARROW_RE2_LINKAGE': 'static',
    # Build Arrow libraries with RATH set to $ORIGIN
    # 'ARROW_RPATH_ORIGIN': 'OFF',
    # Build Arrow with TensorFlow support enabled
    ARROW_TENSORFLOW='OFF',
    # Linkage of Arrow libraries with unit tests executables. static|shared
    ARROW_TEST_LINKAGE='shared',
    # Run the test suite using valgrind --tool=memcheck
    # 'ARROW_TEST_MEMCHECK': 'OFF',
    # Enable Address Sanitizer checks
    # 'ARROW_USE_ASAN': 'OFF',
    # Use ccache when compiling (if available)
    # 'ARROW_USE_CCACHE': 'ON',
    # Build libraries with glog support for pluggable logging
    # 'ARROW_USE_GLOG': 'ON',
    # Use ld.gold for linking on Linux (if available)
    # 'ARROW_USE_LD_GOLD': 'OFF',
    # Build with SIMD optimizations
    # 'ARROW_USE_SIMD': 'ON',
    # Enable Thread Sanitizer checks
    # 'ARROW_USE_TSAN': 'OFF',
    # If off, 'quiet' flags will be passed to linting tools
    # 'ARROW_VERBOSE_LINT': 'OFF',
    # If off, output from ExternalProjects will be logged to files rather
    # than shown
    ARROW_VERBOSE_THIRDPARTY_BUILD='ON',
    # Build with backtrace support
    ARROW_WITH_BACKTRACE='ON',
    # Build with Brotli compression
    ARROW_WITH_BROTLI='ON',
    # Build with BZ2 compression
    ARROW_WITH_BZ2='OFF',
    # Build with lz4 compression
    ARROW_WITH_LZ4='ON',
    # Build with Snappy compression
    ARROW_WITH_SNAPPY='ON',
    # Build with zlib compression
    ARROW_WITH_ZLIB='ON',
    # Build with zstd compression, turned off until
    # https://issues.apache.org/jira/browse/ARROW-4831 is resolved
    ARROW_WITH_ZSTD='ON',
    # Build the Parquet examples. Requires static libraries to be built.
    PARQUET_BUILD_EXAMPLES='OFF',
    # Build the Parquet executable CLI tools.
    # Requires static libraries to be built.
    PARQUET_BUILD_EXECUTABLES='OFF',
    # Depend only on Thirdparty headers to build libparquet.
    # Always OFF if building binaries
    PARQUET_MINIMAL_DEPENDENCY='OFF'
)
definitions = {k: util.Property(k, default=v) for k, v in definitions.items()}

ld_library_path = util.Interpolate(
    '%(prop:CMAKE_INSTALL_PREFIX)s/%(prop:CMAKE_INSTALL_LIBDIR)s'
)
arrow_test_data_path = util.Interpolate(
    '%(prop:builddir)s/testing/data'
)
parquet_test_data_path = util.Interpolate(
    '%(prop:builddir)s/cpp/submodules/parquet-testing/data'
)

cpp_mkdir = Mkdir(
    dir='cpp/build',
    name='Create C++ build directory'
)
cpp_cmake = CMake(
    path='..',
    workdir='cpp/build',
    generator='Ninja',
    definitions=definitions
)
cpp_compile = Ninja(
    j=util.Property('ncpus', 6),
    name='Compile C++',
    workdir='cpp/build'
)
cpp_test = CTest(
    j=util.Property('ncpus', 6),
    output_on_failure=True,
    workdir='cpp/build'
)
cpp_install = Ninja(
    'install',
    name='Install C++',
    workdir='cpp/build'
)
python_install = SetupPy(
    args=['develop'],
    name='Build PyArrow',
    workdir='python',
    env=dict(
        ARROW_HOME=util.Property('CMAKE_INSTALL_PREFIX'),
        PYARROW_CMAKE_GENERATOR=util.Property('CMAKE_GENERATOR'),
        PYARROW_BUILD_TYPE=util.Property('CMAKE_BUILD_TYPE'),
        PYARROW_WITH_ORC=util.Property('ARROW_ORC'),
        PYARROW_WITH_CUDA=util.Property('ARROW_CUDA'),
        PYARROW_WITH_FLIGHT=util.Property('ARROW_FLIGHT'),
        PYARROW_WITH_PLASMA=util.Property('ARROW_PLASMA'),
        PYARROW_WITH_GANDIVA=util.Property('ARROW_GANDIVA'),
        PYARROW_WITH_PARQUET=util.Property('ARROW_PARQUET'),
    )
)
python_test = PyTest(
    name='Test PyArrow',
    args=['pyarrow'],
    workdir='python',
    env={'LD_LIBRARY_PATH': ld_library_path}
)
r_deps = R(
    args=[
        '-e',
        textwrap.dedent("""
            install.packages(
                "remotes",
                repo = "http://cran.rstudio.com/"
            )
            remotes::install_deps(
                dependencies = TRUE,
                upgrade = "never",
                repos = "https://cran.rstudio.com"
            )
        """)
    ],
    name='Install dependencies',
    workdir='r'
)
r_build = R(
    args=['CMD', 'build', '.'],
    name='Build',
    workdir='r'
)
r_install = R(
    args=['CMD', 'INSTALL', 'arrow_*tar.gz'],
    as_shell=True,
    name='Install',
    workdir='r',
    env={
        'LD_LIBRARY_PATH': ld_library_path
    }
)
r_check = R(
    args=['CMD', 'check', 'arrow_*tar.gz', '--no-manual'],
    as_shell=True,  # to expand *
    name='Check',
    workdir='r',
    env={
        'LD_LIBRARY_PATH': ld_library_path,
        '_R_CHECK_FORCE_SUGGESTS_': 'false'
    }
)


class CrossbowTrigger(DockerBuilder):
    """Trigger crossbow builds

    The crossbow tool is hosted within arrow, so we need to clone both arrow
    and the crossbow repository which serves as a queue for 3rdparty CI
    services like Travis or CircleCI. Then using crossbow's command line
    interface it triggers builds by adding new branches to the crossbow
    repository.
    This builder is driven via buildbot properties, the `crossbow_args`
    property is set by the github hook which parses the github comments like
    `@ursabot package -g conda`. For more see commands.py.
    """
    tags = ['crossbow']
    env = dict(
        GIT_COMMITTER_NAME='ursabot',
        GIT_COMMITTER_EMAIL='ursabot@ci.ursalabs.org'
    )
    steps = [
        GitHub(
            name='Clone Arrow',
            repourl=util.Property('repository'),
            workdir='arrow',
            mode='full'
        ),
        GitHub(
            name='Clone Crossbow',
            repourl=util.Property('crossbow_repo'),
            workdir='crossbow',
            branch='master',
            mode='full',
            # quite misleasing option, but it prevents checking out the branch
            # set in the sourcestamp by the pull request, which refers to arrow
            alwaysUseLatest=True
        ),
        Crossbow(
            args=util.FlattenList([
                '--github-token', util.Secret('ursabot/github_token'),
                'submit',
                '--output', 'job.yml',
                '--job-prefix', 'ursabot',
                '--arrow-remote', util.Property('repository'),
                util.Property('crossbow_args', [])
            ]),
            workdir='arrow/dev/tasks',
            result_file='job.yml'
        )
    ]
    image_filter = Filter(
        name='crossbow',
        tag='worker'
    )


class CppBenchmark(DockerBuilder):
    """Run C++ benchmarks via the Archery CLI tool

    This builder is parametrized with builtbot properties which are set by
    the github hook, for more see commands.py
    """
    tags = ['arrow', 'cpp', 'benchmark']
    properties = dict(
        CMAKE_INSTALL_PREFIX='/usr/local',
        CMAKE_INSTALL_LIBDIR='lib'
    )
    steps = [
        checkout_arrow,
        Pip(['install', '-e', '.'], workdir='dev/archery'),
        Archery(
            args=util.FlattenList([
                'benchmark',
                'diff',
                '--output=diff.json',
                util.Property('benchmark_options', []),
                'WORKSPACE',
                util.Property('benchmark_baseline', 'master')
            ]),
            result_file='diff.json'
        )
    ]
    image_filter = Filter(
        name='cpp-benchmark',
        tag='worker',
        variant=None,  # plain linux images, not conda
        platform=Filter(
            arch='amd64',  # until ARROW-5382: SSE on ARM NEON gets resolved
            distro='ubuntu'
        )
    )


class CppTest(DockerBuilder):
    tags = ['arrow', 'cpp', 'parquet', 'plasma']
    volumes = [
        util.Interpolate('%(prop:builddir)s:/root/.ccache:rw')
    ]
    properties = dict(
        ARROW_PARQUET='ON',
        ARROW_PLASMA='ON',
        CMAKE_INSTALL_PREFIX='/usr/local',
        CMAKE_INSTALL_LIBDIR='lib'
    )
    env = {
        'PARQUET_TEST_DATA': parquet_test_data_path  # for parquet
    }
    steps = [
        checkout_arrow,
        cpp_mkdir,
        cpp_cmake,
        cpp_compile,
        cpp_install,
        cpp_test
    ]
    image_filter = Filter(
        name='cpp',
        tag='worker',
        variant=None,
        platform=Filter(
            arch=AnyOf('amd64', 'arm64v8'),
            distro='ubuntu'
        )
    )


class CppCudaTest(CppTest):
    tags = Extend(['cuda'])
    hostconfig = {
        'runtime': 'nvidia'
    }
    properties = Merge(
        ARROW_CUDA='ON'
    )
    worker_filter = Filter(
        tags=Has('cuda')
    )
    image_filter = Filter(
        name='cpp',
        tag='worker',
        variant='cuda',
        platform=Filter(
            arch='amd64'
        )
    )


class RTest(CppTest):
    tags = Extend(['r'])
    steps = Extend([
        # runs the C++ tests too
        r_deps,
        r_build,
        r_install,
        r_check
    ])
    image_filter = Filter(
        name='r',
        tag='worker',
        variant=None,  # plain linux images, not conda
        platform=Filter(
            arch='amd64'
        )
    )


class PythonTest(CppTest):
    tags = Extend(['python'])
    hostconfig = dict(
        shm_size='2G',  # required for plasma
    )
    properties = Merge(
        ARROW_PYTHON='ON'
    )
    steps = Extend([
        python_install,
        python_test
    ])
    image_filter = Filter(
        name=Matching('python*'),
        tag='worker',
        variant=None,  # plain linux images, not conda
        platform=Filter(
            arch=AnyOf('amd64', 'arm64v8'),
            distro='ubuntu'
        )
    )


class PythonDockerTest(PythonTest, DockerBuilder):
    hostconfig = dict(
        shm_size='2G',  # required for plasma
    )
    image_filter = Filter(
        name=Matching('python*'),
        tag='worker',
        variant=None,  # plain linux images, not conda
        platform=Filter(
            arch=AnyOf('amd64', 'arm64v8'),
            distro='ubuntu'
        )
    )


class PythonCudaTest(PythonTest):
    tags = Extend(['cuda'])
    hostconfig = dict(
        shm_size='2G',  # required for plasma
        runtime='nvidia',  # required for cuda
    )
    properties = Merge(
        ARROW_CUDA='ON',  # also sets PYARROW_WITH_CUDA
    )
    worker_filter = Filter(
        tags=Has('cuda')
    )
    image_filter = Filter(
        name=Matching('python*'),
        tag='worker',
        variant='cuda',
        platform=Filter(
            arch='amd64'
        )
    )


def as_system_includes(stdout, stderr):
    """Parse the output of `c++ -E -Wp,-v -xc++ -`"""
    args = []
    for line in stderr.splitlines():
        if line.startswith(' '):
            args.extend(('-isystem', line.strip()))
    return ';'.join(args)


class CppCondaTest(DockerBuilder):
    tags = ['arrow', 'cpp', 'flight', 'gandiva', 'parquet', 'plasma']
    volumes = [
        util.Interpolate('%(prop:builddir)s:/root/.ccache:rw')
    ]
    properties = dict(
        ARROW_FLIGHT='ON',
        ARROW_PLASMA='ON',
        ARROW_PARQUET='ON',
        ARROW_GANDIVA='ON',
        CMAKE_INSTALL_LIBDIR='lib'
    )
    env = dict(
        ARROW_TEST_DATA=arrow_test_data_path,  # for flight
        PARQUET_TEST_DATA=parquet_test_data_path  # for parquet
    )
    steps = [
        SetPropertiesFromEnv(dict(
            CXX='CXX',
            CMAKE_AR='AR',
            CMAKE_RANLIB='RANLIB',
            CMAKE_INSTALL_PREFIX='CONDA_PREFIX',
            ARROW_BUILD_TOOLCHAIN='CONDA_PREFIX'
        )),
        # pass system includes paths to clang
        SetPropertyFromCommand(
            'ARROW_GANDIVA_PC_CXX_FLAGS',
            extract_fn=as_system_includes,
            command=[util.Property('CXX', 'c++')],
            args=['-E', '-Wp,-v', '-xc++', '-'],
            collect_stdout=False,
            collect_stderr=True,
            workdir='.'
        ),
        checkout_arrow,
        cpp_mkdir,
        cpp_cmake,
        cpp_compile,
        cpp_install,
        cpp_test
    ]
    image_filter = Filter(
        name='cpp',
        variant='conda',
        tag='worker'
    )


class RCondaTest(CppCondaTest):
    tags = Extend(['r'])
    steps = Extend([
        r_deps,
        r_build,
        r_install,
        r_check
    ])
    image_filter = Filter(
        name='r',
        variant='conda',
        tag='worker'
    )


class PythonCondaTest(CppCondaTest):
    tags = Extend(['python'])
    hostconfig = dict(
        shm_size='2G',  # required for plasma
    )
    properties = Merge(
        ARROW_PYTHON='ON'
    )
    steps = Extend([
        python_install,
        python_test
    ])
    image_filter = Filter(
        name=Matching('python*'),
        variant='conda',
        tag='worker'
    )


class JavaTest(DockerBuilder):
    tags = ['arrow', 'java']
    steps = [
        checkout_arrow,
        Maven(
            args=['-B', 'test'],
            workdir='java',
            name='Maven Test',
        )
    ]
    image_filter = Filter(
        name=Matching('java*'),
        tag='worker',
        platform=Filter(
            arch='amd64'
        )
    )


class JSTest(DockerBuilder):
    tags = ['arrow', 'js']
    volumes = [
        util.Interpolate('%(prop:builddir)s:/root/.npm:rw')
    ]
    steps = [
        checkout_arrow,
        Npm(['install', '-g', 'npm@latest'], workdir='js', name='Update NPM'),
        Npm(['install'], workdir='js', name='Install Dependencies'),
        Npm(['run', 'lint'], workdir='js', name='Lint'),
        Npm(['run', 'build'], workdir='js', name='Build'),
        Npm(['run', 'test'], workdir='js', name='Test')
    ]
    image_filter = Filter(
        name=Matching('js*'),
        tag='worker',
        platform=Filter(
            arch='amd64'
        )
    )


class GoTest(DockerBuilder):
    tags = ['arrow', 'go']
    env = {
        'GO111MODULE': 'on',
    }
    steps = [
        checkout_arrow,
        Go(
            args=['get', '-v', '-t', './...'],
            workdir='go/arrow',
            name='Go Build',
        ),
        Go(
            args=['test', './...'],
            workdir='go/arrow',
            name='Go Test',
        )
    ]
    image_filter = Filter(
        name=Matching('go*'),
        tag='worker',
        platform=Filter(
            arch='amd64'
        )
    )


class RustTest(DockerBuilder):
    tags = ['arrow', 'rust']
    env = dict(
        ARROW_TEST_DATA=arrow_test_data_path,
        PARQUET_TEST_DATA=parquet_test_data_path
    )
    steps = [
        checkout_arrow,
        Cargo(
            args=['build'],
            workdir='rust',
            name='Rust Build'
        ),
        Cargo(
            args=['test'],
            workdir='rust',
            name='Rust Test'
        )
    ]
    image_filter = Filter(
        name=Matching('rust*'),
        tag='worker',
        platform=Filter(
            arch='amd64'
        )
    )
