## Installing pyarrow (Apache Arrow Python library)

First, clone the master git repository:

```bash
git clone https://github.com/apache/arrow.git arrow
```

#### System requirements

Building pyarrow requires:

* A C++11 compiler

  * Linux: gcc >= 4.8 or clang >= 3.5
  * OS X: XCode 6 or higher

* [cmake][1]

#### Python requirements

You will need Python (CPython) 2.7, 3.4, or 3.5 installed. Earlier releases and
are not being targeted.

> This library targets CPython only due to an emphasis on interoperability with
> pandas and NumPy, which are only available for CPython.

The build requires NumPy, Cython, and a few other Python dependencies:

```bash
pip install cython
cd arrow/python
pip install -r requirements.txt
```

#### Installing Arrow C++ library

First, you should choose an installation location for Arrow C++. In the future
using the default system install location will work, but for now we are being
explicit:

```bash
export ARROW_HOME=$HOME/local
```

Now, we build Arrow:

```bash
cd arrow/cpp

mkdir dev-build
cd dev-build

cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME ..

make

# Use sudo here if $ARROW_HOME requires it
make install
```

#### Build and install `pyarrow` library

```bash
cd arrow/python

python setup.py install
```

#### Mac OS X-specific stuff

[1]: https://cmake.org/