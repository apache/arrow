from typing import Iterable

import pyarrow as pa


INF_ROWS_PER_BATCH = 0
"""
The infimum amount of rows per batch.
"""
MIN_ROWS_PER_BATCH = 0
"""
The minimum amount of rows per batch.
"""
MAX_ROWS_PER_BATCH = 2 ** (10 + 10)
"""
The maximum amount of rows per batch, defaults to 2**20 (~10**6) rows per batch.
"""
SUP_ROWS_PER_BATCH = 2**63 - 1
"""
The supremum amount of rows per batch.
"""

INF_BYTES_PER_BATCH = 0
"""
The infimum amount of bytes per batch.
"""
MIN_BYTES_PER_BATCH = 0
"""
The minimum amount of bytes per batch.
"""
MAX_BYTES_PER_BATCH = 2 ** (9 + 10 + 10)
"""
The maximum amount of bytes per batch, defaults to 2**19 bytes (=512 MiB) per batch.
"""
SUP_BYTES_PER_BATCH = 2**63 - 1
"""
The supremum amount of bytes per batch.
"""


def _scatter_batches(
    iterable: Iterable[pa.RecordBatch],
) -> Iterable[pa.RecordBatch]:
    """
    Transforms an iterable of pa.RecordBatch of arbitrary amount of rows to an iterable
    of pa.RecordBatch of one single row. It does not duplicate the underlying data,
    but merely returns zero-copy slices.

    :param iterable: An iterable of pa.RecordBatch.
    :return: An iterable of pa.RecordBatch.
    """
    for record_batch in iterable:
        for index in range(record_batch.num_rows):
            yield record_batch.slice(offset=index, length=1)


def restack_batches(
    iterable: Iterable[pa.RecordBatch],
    min_rows_per_batch: int = MIN_ROWS_PER_BATCH,
    max_rows_per_batch: int = MAX_ROWS_PER_BATCH,
    min_bytes_per_batch: int = MIN_BYTES_PER_BATCH,
    max_bytes_per_batch: int = MAX_BYTES_PER_BATCH,
) -> Iterable[pa.RecordBatch]:
    """
    Restack an input iterable, such that the output iterable contains pa.RecordBatch
    which fit the constraints w.r.t. the amount of rows and bytes.
    Note, if a single row has more bytes than the maximum, or if the final batch had
    have fewer rows or bytes than either of the minimums, it will be yielded as is.

    :param iterable: An iterable of pa.RecordBatch.
    :param min_rows_per_batch: The minimum amount of rows per batch.
    :param max_rows_per_batch: The maximum amount of rows per batch.
    :param min_bytes_per_batch: The minimum amount of bytes per batch.
    :param max_bytes_per_batch: The maximum amount of bytes per batch.
    :return: An iterable of pa.RecordBatch.
    """
    assert (
        INF_ROWS_PER_BATCH
        <= min_rows_per_batch
        <= max_rows_per_batch
        <= SUP_ROWS_PER_BATCH
    )
    assert (
        INF_BYTES_PER_BATCH
        <= min_bytes_per_batch
        <= max_bytes_per_batch
        <= SUP_BYTES_PER_BATCH
    )
    # we initialize the output_batch
    output_batch, num_rows_in_output, num_bytes_in_output = [], 0, 0
    for input_batch in _scatter_batches(iterable=iterable):
        # input_batch should always have a single row
        num_rows_in_input = input_batch.num_rows
        num_bytes_in_input = input_batch.nbytes
        # we have "enough" rows in the output_batch if we have more than the minimum
        # amount of rows and that adding the input_batch would make us go over the
        # maximum amount of rows.
        enough_rows_already = (
            min_rows_per_batch <= num_rows_in_output
            and max_rows_per_batch < (num_rows_in_input + num_rows_in_output)
        )
        # we have "enough" bytes in the output_batch if we have more than the minimum
        # amount of bytes and that adding the input_batch would make us go over the
        # maximum amount of bytes.
        enough_bytes_already = (
            min_bytes_per_batch <= num_bytes_in_output
            and max_bytes_per_batch < (num_bytes_in_input + num_bytes_in_output)
        )
        # we also need to verify that the output_batch is not empty before yielding
        if (enough_rows_already or enough_bytes_already) and output_batch:
            # we concatenate and yield the output_batch
            output_batch = pa.concat_batches(output_batch)
            yield output_batch
            # we re-initialize the output_batch
            output_batch, num_rows_in_output, num_bytes_in_output = [], 0, 0
        # we append to the output_batch and update the amount of rows and bytes.
        output_batch.append(input_batch)
        num_rows_in_output += num_rows_in_input
        num_bytes_in_output += num_bytes_in_input
    # if the final output_batch is not empty, we need to yield it
    if output_batch:
        output_batch = pa.concat_batches(output_batch)
        yield output_batch
