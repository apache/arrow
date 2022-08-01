.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. default-domain:: cpp
.. highlight:: cpp

=====================
Arrow Skyhook example
=====================

The file ``cpp/examples/arrow/dataset_skyhook_scan_example.cc``
located inside the source tree contains an example of using Skyhook to
offload filters and projections to a Ceph cluster.

Instuctions
===========

.. note::
   The instructions below are for Ubuntu 20.04 or above.

1. Install Ceph and Skyhook dependencies.

   .. code-block:: bash

      apt update
      apt install -y cmake \
                     libradospp-dev \
                     rados-objclass-dev \
                     ceph \
                     ceph-common \
                     ceph-osd \
                     ceph-mon \
                     ceph-mgr \
                     ceph-mds \
                     rbd-mirror \
                     ceph-fuse \
                     rapidjson-dev \
                     libboost-all-dev \
                     python3-pip

2. Build and install Skyhook.

   .. code-block:: bash

      git clone https://github.com/apache/arrow
      cd arrow/
      mkdir -p cpp/release
      cd cpp/release
      cmake -DARROW_SKYHOOK=ON \
            -DARROW_PARQUET=ON \
            -DARROW_WITH_SNAPPY=ON \
            -DARROW_BUILD_EXAMPLES=ON \
            -DARROW_DATASET=ON \
            -DARROW_CSV=ON \
            -DARROW_WITH_LZ4=ON \
            ..

      make -j install
      cp release/libcls_skyhook.so /usr/lib/x86_64-linux-gnu/rados-classes/

3. Deploy a Ceph cluster with a single in-memory OSD using `this <https://github.com/uccross/skyhookdm/blob/master/scripts/micro-osd.sh>`_ script.

   .. code-block:: bash

      ./micro-osd.sh /tmp/skyhook

4. Generate the example dataset.

   .. code-block:: bash

      pip install pandas pyarrow
      python3 ../../ci/scripts/generate_dataset.py
      cp -r nyc /mnt/cephfs/

5. Execute the example.

   .. code-block:: bash

      LD_LIBRARY_PATH=/usr/local/lib release/dataset-skyhook-scan-example file:///mnt/cephfs/nyc
