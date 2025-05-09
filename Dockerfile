# Step 1: Use the Apache Arrow development image as base for the build stage
FROM docker.io/apache/arrow-dev:amd64-ubuntu-jammy-package-apache-arrow as arrow-dev-image

# Set environment variable for runtime
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Set working directory
WORKDIR /arrowdev

# Clone Arrow source and check out specific commit
RUN git clone https://github.com/apache/arrow.git . && \
    git checkout 409a016e5fdfa28cabd580b7ec81c42991c0748e

# Build Arrow C++ libraries
RUN cd /arrowdev/cpp && \
    rm -rf build && \
    mkdir build && cd build && \
    cmake -GNinja \
      -DARROW_WITH_SNAPPY=ON \
      -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_INSTALL_PREFIX=$(pwd)/install \
      -DARROW_PARQUET=ON \
      -DPARQUET_REQUIRE_ENCRYPTION=ON \
      -DARROW_PYTHON=ON \
      -DARROW_COMPUTE=ON \
      .. && \
    ninja install

# Set environment variables for Arrow and Parquet functionality
ENV CMAKE_PREFIX_PATH=/arrowdev/cpp/build/install
ENV ARROW_HOME=/arrowdev/cpp/build/install
ENV LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH

# Build Python bindings
WORKDIR /arrowdev/python
RUN pip install -r requirements-build.txt && \
    rm -rf build/ && \
    python3 setup.py build_ext --inplace

# Test basic Parquet functionality
RUN python3 -c "import pyarrow.parquet as pq; print('Parquet OK')"

# Test Parquet encryption functionality
RUN python3 -c "import pyarrow.parquet.encryption as ppe; print('Parquet encryption OK')"

# Default shell for the build stage
CMD ["bash"]
