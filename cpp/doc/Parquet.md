## Building Arrow-Parquet integration

To use Arrow C++ with Parquet, you must first build the Arrow C++ libraries and
install them someplace. Then, you can build [parquet-cpp][1] with the Arrow
adapter library:

```bash
# Set this to your preferred install location
export ARROW_HOME=$HOME/local

git clone https://github.com/apache/parquet-cpp.git
cd parquet-cpp
source setup_build_env.sh
cmake -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME -DPARQUET_ARROW=on
make -j4
make install
```

[1]: https://github.com/apache/parquet-cpp