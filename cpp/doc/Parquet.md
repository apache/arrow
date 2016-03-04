## Building Arrow-Parquet integration

To build the Arrow C++'s Parquet adapter library, you must first build [parquet-cpp][1]:

```bash
# Set this to your preferred install location
export PARQUET_HOME=$HOME/local

git clone https://github.com/apache/parquet-cpp.git
cd parquet-cpp
source setup_build_env.sh
cmake -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME
make -j4
make install
```

Make sure that `$PARQUET_HOME` is set to the installation location. Now, build
Arrow with the Parquet adapter enabled:

```bash
cmake -DARROW_PARQUET=ON
```

[1]: https://github.com/apache/parquet-cpp