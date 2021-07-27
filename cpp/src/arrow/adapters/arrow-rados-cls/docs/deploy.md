<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Installing Ceph and SkyhookDM

1. Execute [this](../scripts/deploy_ceph.sh) script to deploy a Ceph cluster on a set of nodes and to mount CephFS on the client/admin node. For example, on the client node, execute
```bash
./deploy_ceph.sh mon1,mon2,mon3 osd1,osd2,osd3 mds1 mgr1
```
where mon1, mon2, osd1, etc. are the internal hostnames of the nodes.

2. Build and install the SkyhookDM CLS plugins and PyArrow (with Skyhook extensions) using [this](../scripts/deploy_skyhook.sh) script. For example,
```bash
./deploy_skyhook.sh osd1,osd2,osd3
```
This will build the CLS plugins as shared libraries and deploy them to the OSD nodes.

3. Optionally, we can also deploy Prometheus and Grafana for monitoring the cluster by following [this](https://github.com/JayjeetAtGithub/prometheus-on-baremetal) guide.

# Interacting with SkyhookDM

1. Download some sample Parquet files to the admin node.
```bash
apt update
apt install git-lfs
git clone https://github.com/JayjeetAtGithub/datasets
cd datasets/
git lfs pull
``` 

2. Create and write a sample dataset to the CephFS mount using [this](../scripts/deploy_data.sh) script by replicating the 128MB Parquet file downloaded in Step 1.
```bash
./deploy_data.sh datasets/128MB.parquet /mnt/cephfs/dataset 100 134217728
```
This will write 100 of ~128MB Parquet files to `/mnt/cephfs/dataset` using a CephFS stripe size of 128MB.

3. Write a client script and get started with querying datasets in SkyhookDM. An example script is given below.
```python
import pyarrow.dataset as ds
mydataset = ds.dataset("file:///mnt/cephfs/dataset", format="skyhook")
print(mydataset.to_table())
```
