## Python library for Apache Arrow

This library provides a Pythonic API wrapper for the reference Arrow C++
implementation, along with tools for interoperability with pandas, NumPy, and
other traditional Python scientific computing packages.

### Development details

This project is layered in two pieces:

* pyarrow, a C++ library for easier interoperability between Arrow C++, NumPy,
  and pandas
* Cython extensions and pure Python code under arrow/ which expose Arrow C++
  and pyarrow to pure Python users

#### PyArrow Installation
These are instructions on how to install PyArrow from scratch on Linux (assuming arrow is not yet installed)

1. **g++ and gcc**

  Make sure the latest versions of g++ and gcc are installed.
  ```bash
  sudo add-apt-repository ppa:ubuntu-toolchain-r/test
  sudo apt-get update
  sudo apt-get install gcc-4.9 g++-4.9
  ```

2. **cmake**
  ```bash
  sudo add-apt-repository ppa:george-edison55/cmake-3.x
  sudo apt-get update
  sudo apt-get install cmake
  ```

3. **boost**
  ```bash
  sudo apt-get install build-essential g++ python-dev autotools-dev libicu-dev build-essential libbz2-dev
  wget -O boost_1_60_0.tar.gz http://sourceforge.net/projects/boost/files/boost/1.60.0/boost_1_60_0.tar.gz/download
  tar xzvf boost_1_60_0.tar.gz
  cd boost_1_60_0
  ./bootstrap.sh --prefix=/usr/local
  sudo ./b2 install
  ```

3. **miniconda (optional)**

  Installing mininconda is optional but is recommended to easily install parquet-cpp and other dependencies (see below). Skip this if you prefer installing parquet-cpp from source.
  ```bash
  # Assumes that you are in the root of the arrow project.
  export HOME=$(pwd)
  # Change these if you would like to mininconda to reside in a different location.
  export MINICONDA=$HOME/miniconda

  wget -O miniconda.sh https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh

  bash miniconda.sh -b -p $MINICONDA
  export PATH="$MINICONDA/bin:$PATH"

  conda update -y -q conda
  conda config --set show_channel_urls True
  conda config --add channels https://repo.continuum.io/pkgs/free
  conda config --add channels conda-forge
  conda config --add channels apache
  conda info -a

  conda install --yes conda-build jinja2 anaconda-client
  conda install -y nomkl
  ```

5. **Parquet-cpp**

  If you would like to install parquet-cpp from source, (https://github.com/apache/parquet-cpp/blob/master/README.md) is a better place to look at. You need to set the ``PARQUET_HOME`` environment variable to where parquet-cpp is installed.
  ```bash
  conda install -y --channel apache/channel/dev parquet-cpp
  export PARQUET_HOME=$MINICONDA
  ```

6. **Arrow-cpp and its dependencies***

  We need arrow-cpp for its python port. If you have already have arrow-cpp, just remember to set the environment variable
  ``ARROW_CPP_INSTALL`` to wherever it is installed.
  ```bash
  export CPP_BUILD_DIR=$HOME/cpp-build
  mkdir $CPP_BUILD_DIR
  cd $CPP_BUILD_DIR
  export CPP_DIR=$HOME/cpp

  cp -r $CPP_DIR/thirdparty .
  cp $CPP_DIR/setup_build_env.sh .
  source setup_build_env.sh

  # Change this if you want arrow to be installed elsewhere.
  export ARROW_CPP_INSTALL=$HOME/cpp-install

  CMAKE_COMMON_FLAGS="\
  -DARROW_BUILD_BENCHMARKS=ON \
  -DARROW_PARQUET=ON \
  -DARROW_HDFS=on \
  -DCMAKE_INSTALL_PREFIX=$ARROW_CPP_INSTALL"
  cmake -DARROW_TEST_MEMCHECK=on \
        $CMAKE_COMMON_FLAGS \
        -DCMAKE_CXX_FLAGS="-Werror" \
        $CPP_DIR

  make -j4
  make install

  cd..
  ```

7. **Install python dependencies**

  If you have installed miniconda follow this. Else you could install them as you wish.
  ```bash
  PYTHON_DIR=$HOME/python
  export LD_LIBRARY_PATH="$MINICONDA/lib:LD_LIBRARY_PATH"
  conda install -y numpy pandas cython pytest
  pushd $PYTHON_DIR
  ```

8. **Build pyarrow**

  ```bash
  python setup.py build_ext --inplace
  ```
