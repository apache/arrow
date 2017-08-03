from multiprocessing import Pool
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
import subprocess
import sys
import time

import multimerge

client = None
num_cores = 8
num_rows = 200000
num_cols = 2
column_names = [str(i) for i in range(num_cols)]
column_name = column_names[0]


# Connect to clients
def connect():
    global client
    client = plasma.connect('/tmp/store', '', 0)
    np.random.seed(int(time.time() * 10e7) % 10000000)


def put_df(df):
    record_batch = pa.RecordBatch.from_pandas(df)

    # Get size of record batch and schema
    mock_sink = pa.MockOutputStream()
    stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
    stream_writer.write_batch(record_batch)
    data_size = mock_sink.size()

    # Generate an ID and allocate a buffer in the object store for the
    # serialized DataFrame
    object_id = plasma.ObjectID(np.random.bytes(20))
    buf = client.create(object_id, data_size)

    # Write the serialized DataFrame to the object store
    sink = pa.FixedSizeBufferOutputStream(buf)
    stream_writer = pa.RecordBatchStreamWriter(sink, record_batch.schema)
    stream_writer.write_batch(record_batch)

    # Seal the object
    client.seal(object_id)

    return object_id


def get_df(object_id):
    """Retrieve a dataframe from the object store given its object ID."""
    buf = client.get([object_id])[0]
    reader = pa.RecordBatchStreamReader(buf)
    record_batch = reader.read_next_batch()
    return record_batch.to_pandas()


def local_sort(object_id):
    """Sort a partition of a dataframe."""
    # Get the dataframe from the object store.
    df = get_df(object_id)
    # Sort the dataframe.
    sorted_df = df.sort_values(by=column_name)
    # Get evenly spaced values from the dataframe.
    indices = np.linspace(0, len(df) - 1, num=num_cores, dtype=np.int64)
    # Put the sorted dataframe in the object store and return the corresponding
    # object ID as well as the sampled values.
    return put_df(sorted_df), sorted_df.as_matrix().take(indices)


def local_partitions(object_id_and_pivots):
    """Take a sorted partition of a dataframe and split it into more pieces."""
    object_id, pivots = object_id_and_pivots
    df = get_df(object_id)
    split_at = df[column_name].searchsorted(pivots)
    split_at = [0] + list(split_at) + [len(df)]
    # Partition the sorted dataframe and put each partition into the object
    # store.
    return [put_df(df[i:j]) for i, j in zip(split_at[:-1], split_at[1:])]


def merge(object_ids):
    # We could change this to do only one IPC roundtrip to the object store if
    # we want to.
    dfs = [get_df(object_id) for object_id in object_ids]

    arrays = [df.as_matrix() for df in dfs]
    for a in arrays:
        assert a.dtype == np.float64

    s1 = time.time()

    total_size = sum([len(df[column_name]) for df in dfs])

    if total_size == 0:
        return None

    resulting_array = multimerge.multimerge2d(*arrays)
    merged_df = pd.DataFrame(resulting_array, columns=column_names)

    #merged_df = pd.DataFrame(np.zeros((total_size, len(column_names))), columns=column_names)

    s2 = time.time()

    # indices = [0 for i in range(len(dfs))]
    # priority_queue = queue.PriorityQueue()
    # for df_index in range(len(dfs)):
    #     if len(dfs[df_index]) > 0:
    #         priority_queue.put((dfs[df_index][column_name].iloc[indices[df_index]], df_index))
    #
    s3 = time.time()
    #
    # total_index = 0
    # while not priority_queue.empty():
    #     value, df_index = priority_queue.get()
    #     merged_df.iloc[total_index] = dfs[df_index].iloc[indices[df_index]]
    #     indices[df_index] += 1
    #     total_index += 1
    #     if not indices[df_index] == len(dfs[df_index]):
    #         priority_queue.put((dfs[df_index][column_name].iloc[indices[df_index]], df_index))

    s4 = time.time()

    return put_df(merged_df), (s2 - s1, s3 - s2, s4 - s3)


if __name__ == '__main__':
    # Start the plasma store.
    plasma_store_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../../../build/debug/plasma_store')
    p = subprocess.Popen([plasma_store_path, '-s', '/tmp/store', '-m', '500000000000'])

    # Connect to the plasma store.
    connect()

    # Connect the processes in the pool.
    pool = Pool(initializer=connect, initargs=(), processes=num_cores)

    df = pd.DataFrame(np.random.randn(num_rows, num_cols), columns=column_names)

    partition_ids = [put_df(partition) for partition in np.split(df, num_cores)]

    t1 = time.time()

    # Sort each partition and subsample them. The subsampled values will be
    # used to create buckets.
    sorted_df_ids, pivot_groups = list(zip(*pool.map(local_sort, partition_ids)))

    t2 = time.time()

    # Choose the pivots.
    all_pivots = np.concatenate(pivot_groups)

    t3 = time.time()

    indices = np.linspace(0, len(all_pivots) - 1, num=num_cores, dtype=np.int64)

    t4 = time.time()

    pivots = np.take(np.sort(all_pivots), indices)

    t5 = time.time()

    # Break all of the sorted partitions into even smaller partitions. Group
    # the object IDs from each bucket together.
    results = list(zip(*pool.map(local_partitions, zip(sorted_df_ids, len(sorted_df_ids) * [pivots]))))

    t6 = time.time()

    # Merge each of the buckets and store the results in the object store.
    object_ids_and_times = pool.map(merge, results)

    t7 = time.time()

    print('Parallel sort took {} seconds.'.format(t7 - t1))
    print('Breakdown', t2 - t1, t3 - t2, t4 - t3, t5 - t4, t6 - t5, t7 - t6)
    print('Merge times')
    object_ids = []
    for object_ids_time in object_ids_and_times:
        if object_ids_time is None:
            continue
        object_id, times = object_ids_time
        object_ids.append(object_id)
        print(times)

    # Check that we sorted the DataFrame properly.
    sorted_dfs = [get_df(object_id) for object_id in object_ids]
    sorted_df = pd.concat(sorted_dfs)

    time3 = time.time()

    original_sorted_df = df.sort_values(by=column_name)

    time4 = time.time()
    print('Serial sort took {} seconds.'.format(time4 - time3))

    import IPython
    IPython.embed()

    assert np.allclose(sorted_df.values, original_sorted_df.values)

    p.kill()
