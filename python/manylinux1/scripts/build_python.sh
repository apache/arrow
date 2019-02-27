#!/bin/bash -e
# Copyright (c) 2016 manylinux
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# The following is taken from docker/build_scripts/build_env.sh,
# docker/build_scripts/build_utils.sh and
# docker/build_scripts/build.sh from the manylinux1 project
# (https://github.com/pypa/manylinux/).

PYTHON_DOWNLOAD_URL=https://www.python.org/ftp/python
CPYTHON_VERSIONS="2.7.15 3.4.9 3.5.6 3.6.8 3.7.2"

# openssl version to build, with expected sha256 hash of .tar.gz
# archive.
OPENSSL_ROOT=openssl-1.0.2q
OPENSSL_HASH=5744cfcbcec2b1b48629f7354203bc1e5e9b5466998bbccc5b5fcde3b18eb684
OPENSSL_DOWNLOAD_URL=https://www.openssl.org/source

# Update to slightly newer, verified Git commit:
# https://github.com/NixOS/patchelf/commit/2a9cefd7d637d160d12dc7946393778fa8abbc58
PATCHELF_VERSION=2a9cefd7d637d160d12dc7946393778fa8abbc58
PATCHELF_HASH=12da4727f09be42ae0b54878e1b8e86d85cb7a5b595731cdc1a0a170c4873c6d

CURL_ROOT=curl-7.61.1
CURL_HASH=eaa812e9a871ea10dbe8e1d3f8f12a64a8e3e62aeab18cb23742e2f1727458ae
CURL_DOWNLOAD_URL=https://curl.haxx.se/download

AUTOCONF_ROOT=autoconf-2.69
AUTOCONF_HASH=954bd69b391edc12d6a4a51a2dd1476543da5c6bbf05a95b59dc0dd6fd4c2969
AUTOCONF_DOWNLOAD_URL=http://ftp.gnu.org/gnu/autoconf
AUTOMAKE_ROOT=automake-1.16.1
AUTOMAKE_HASH=608a97523f97db32f1f5d5615c98ca69326ced2054c9f82e65bade7fc4c9dea8
AUTOMAKE_DOWNLOAD_URL=http://ftp.gnu.org/gnu/automake
LIBTOOL_ROOT=libtool-2.4.6
LIBTOOL_HASH=e3bd4d5d3d025a36c21dd6af7ea818a2afcd4dfc1ea5a17b39d7854bcd0c06e3
LIBTOOL_DOWNLOAD_URL=http://ftp.gnu.org/gnu/libtool

SQLITE_AUTOCONF_VERSION=sqlite-autoconf-3230100
SQLITE_AUTOCONF_HASH=92842b283e5e744eff5da29ed3c69391de7368fccc4d0ee6bf62490ce555ef25
SQLITE_AUTOCONF_DOWNLOAD_URL=https://www.sqlite.org/2018

GIT_ROOT=2.19.1
GIT_HASH=ba2fed9d02e424b735e035c4f2b0bdb168ef0df7e35156b5051d900dc7247787
GIT_DOWNLOAD_URL=https://github.com/git/git/archive

GET_PIP_URL=https://bootstrap.pypa.io/get-pip.py
EPEL_RPM_HASH=0dcc89f9bf67a2a515bad64569b7a9615edc5e018f676a578d5fd0f17d3c81d4
DEVTOOLS_HASH=a8ebeb4bed624700f727179e6ef771dafe47651131a00a78b342251415646acc

function check_var {
    if [ -z "$1" ]; then
        echo "required variable not defined"
        exit 1
    fi
}

function lex_pyver {
    # Echoes Python version string padded with zeros
    # Thus:
    # 3.2.1 -> 003002001
    # 3     -> 003000000
    echo $1 | awk -F "." '{printf "%03d%03d%03d", $1, $2, $3}'
}

function pyver_dist_dir {
    # Echoes the dist directory name of given pyver, removing alpha/beta prerelease
    # Thus:
    # 3.2.1   -> 3.2.1
    # 3.7.0b4 -> 3.7.0
    echo $1 | awk -F "." '{printf "%d.%d.%d", $1, $2, $3}'
}

function do_cpython_build {
    local py_ver=$1
    check_var $py_ver
    local ucs_setting=$2
    check_var $ucs_setting
    tar -xzf Python-$py_ver.tgz
    pushd Python-$py_ver
    if [ "$ucs_setting" = "none" ]; then
        unicode_flags=""
        dir_suffix=""
    else
        local unicode_flags="--enable-unicode=$ucs_setting"
        local dir_suffix="-$ucs_setting"
    fi
    local prefix="/opt/_internal/cpython-${py_ver}${dir_suffix}"
    mkdir -p ${prefix}/lib
    ./configure --prefix=${prefix} --disable-shared $unicode_flags > /dev/null
    make -j2 > /dev/null
    make install > /dev/null
    popd
    rm -rf Python-$py_ver
    # Some python's install as bin/python3. Make them available as
    # bin/python.
    if [ -e ${prefix}/bin/python3 ]; then
        ln -s python3 ${prefix}/bin/python
    fi
    # --force-reinstall is to work around:
    #   https://github.com/pypa/pip/issues/5220
    #   https://github.com/pypa/get-pip/issues/19
    ${prefix}/bin/python get-pip.py --force-reinstall
    if [ -e ${prefix}/bin/pip3 ] && [ ! -e ${prefix}/bin/pip ]; then
        ln -s pip3 ${prefix}/bin/pip
    fi
    # Since we fall back on a canned copy of get-pip.py, we might not have
    # the latest pip and friends. Upgrade them to make sure.
    ${prefix}/bin/pip install -U --require-hashes -r ${MY_DIR}/requirements.txt
    local abi_tag=$(${prefix}/bin/python ${MY_DIR}/python-tag-abi-tag.py)
    ln -s ${prefix} /opt/python/${abi_tag}
}


function build_cpython {
    local py_ver=$1
    check_var $py_ver
    check_var $PYTHON_DOWNLOAD_URL
    local py_dist_dir=$(pyver_dist_dir $py_ver)
    curl -fsSLO $PYTHON_DOWNLOAD_URL/$py_dist_dir/Python-$py_ver.tgz
    curl -fsSLO $PYTHON_DOWNLOAD_URL/$py_dist_dir/Python-$py_ver.tgz.asc
    if [ $(lex_pyver $py_ver) -lt $(lex_pyver 3.3) ]; then
        do_cpython_build $py_ver ucs2
        do_cpython_build $py_ver ucs4
    else
        do_cpython_build $py_ver none
    fi
    rm -f Python-$py_ver.tgz
    rm -f Python-$py_ver.tgz.asc
}


function build_cpythons {
    check_var $GET_PIP_URL
    curl -fsSLO $GET_PIP_URL
    for py_ver in $@; do
        build_cpython $py_ver
    done
    rm -f get-pip.py
}

function do_openssl_build {
    ./config no-ssl2 no-shared -fPIC --prefix=/usr/local/ssl > /dev/null
    make > /dev/null
    make install_sw > /dev/null
}


function check_required_source {
    local file=$1
    check_var ${file}
    if [ ! -f $file ]; then
        echo "Required source archive must be prefetched to docker/sources/ with prefetch.sh: $file"
        return 1
    fi
}


function fetch_source {
    # This is called both inside and outside the build context (e.g. in Travis) to prefetch
    # source tarballs, where curl exists (and works)
    local file=$1
    check_var ${file}
    local url=$2
    check_var ${url}
    if [ -f ${file} ]; then
        echo "${file} exists, skipping fetch"
    else
        curl -fsSL -o ${file} ${url}/${file}
    fi
}


function check_sha256sum {
    local fname=$1
    check_var ${fname}
    local sha256=$2
    check_var ${sha256}

    echo "${sha256}  ${fname}" > ${fname}.sha256
    sha256sum -c ${fname}.sha256
    rm -f ${fname}.sha256
}


function build_openssl {
    local openssl_fname=$1
    check_var ${openssl_fname}
    local openssl_sha256=$2
    check_var ${openssl_sha256}
    # Can't use curl here because we don't have it yet, OpenSSL must be prefetched
    fetch_source ${openssl_fname}.tar.gz ${OPENSSL_DOWNLOAD_URL}
    check_sha256sum ${openssl_fname}.tar.gz ${openssl_sha256}
    tar -xzf ${openssl_fname}.tar.gz
    (cd ${openssl_fname} && do_openssl_build)
    rm -rf ${openssl_fname} ${openssl_fname}.tar.gz
}

build_openssl $OPENSSL_ROOT $OPENSSL_HASH

mkdir -p /opt/python
build_cpythons $CPYTHON_VERSIONS
