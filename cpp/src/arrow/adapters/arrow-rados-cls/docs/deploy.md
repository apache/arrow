# Installing SkyhookDM

1. If you don't already have a Ceph cluster, please follow [this](https://blog.risingstack.com/ceph-storage-deployment-vm/) guide to create one. 

2. Create and mount CephFS at some path, for example `/mnt/cephfs`.

2. Build and install SkyhookDM and [PyArrow](https://pypi.org/project/pyarrow/) (with Rados Parquet extensions) using [this](https://github.com/JayjeetAtGithub/skyhook-perf-experiments/blob/master/deployment_scripts/skyhook.sh) script.

3. Update your Ceph configuration file with this line.
```
osd class load list = *
```

4. Restart the Ceph OSDs to load the changes.

# Interacting with SkyhookDM

1. Write some [Parquet](https://parquet.apache.org/) files in the CephFS mount.

2. Write a client script and get started with querying datasets in SkyhookDM. An example script is given below.
```python
import pyarrow.dataset as ds

format_ = ds.RadosParquetFileFormat("/path/to/cephconfig", "cephfs-data-pool-name")
dataset_ = ds.dataset("file:///mnt/cephfs/dataset", format=format_)
print(dataset_.to_table())
```
