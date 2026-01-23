import uuid

import pyarrow as pa
import pyarrow.restack as par


def generate_record_batch(n: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"uuid": [uuid.uuid4().bytes for _ in range(n)]})


def _test_restack_batches(
    num_batches: int,
    num_rows_per_batch: int,
    min_rows_per_batch: int,
    max_rows_per_batch: int,
    min_bytes_per_batch: int,
    max_bytes_per_batch: int,
) -> tuple[list[int], list[int]]:
    iterable = (generate_record_batch(n=num_rows_per_batch) for _ in range(num_batches))
    iterable = par.restack_batches(
        iterable=iterable,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
    )
    iterable = ((batch.num_rows, batch.nbytes) for batch in iterable)
    num_rows_batch, num_bytes_batch = zip(*iterable)
    return list(num_rows_batch), list(num_bytes_batch)


def _check_num_rows_batch(
    num_rows_batch: list[int],
    min_rows_per_batch: int,
    max_rows_per_batch: int,
) -> None:
    assert all(
        min_rows_per_batch <= num_rows <= max_rows_per_batch
        for num_rows in num_rows_batch[:-1]
    )
    assert 1 <= num_rows_batch[-1] <= max_rows_per_batch


def _check_num_bytes_batch(
    num_bytes_batch: list[int],
    min_bytes_per_batch: int,
    max_bytes_per_batch: int,
    num_bytes_per_row: int,
) -> None:
    min_bytes_per_batch = min(min_bytes_per_batch, num_bytes_per_row)
    max_bytes_per_batch = max(max_bytes_per_batch, num_bytes_per_row)
    assert all(
        min_bytes_per_batch <= num_bytes <= max_bytes_per_batch
        for num_bytes in num_bytes_batch[:-1]
    )
    assert 0 <= num_bytes_batch[-1] <= max_bytes_per_batch


def test_restack_batches_1():
    num_batches = 1
    num_rows_per_batch = 1
    num_bytes_per_row = generate_record_batch(n=1).nbytes
    min_rows_per_batch = par.MIN_ROWS_PER_BATCH
    max_rows_per_batch = 1
    min_bytes_per_batch = par.MIN_BYTES_PER_BATCH
    max_bytes_per_batch = par.MAX_BYTES_PER_BATCH

    num_rows_batch, num_bytes_batch = _test_restack_batches(
        num_batches=num_batches,
        num_rows_per_batch=num_rows_per_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
    )
    _check_num_rows_batch(
        num_rows_batch=num_rows_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
    )
    _check_num_bytes_batch(
        num_bytes_batch=num_bytes_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
        num_bytes_per_row=num_bytes_per_row,
    )


def test_restack_batches_2():
    num_batches = 1
    num_rows_per_batch = 11
    num_bytes_per_row = generate_record_batch(n=1).nbytes
    min_rows_per_batch = 2
    max_rows_per_batch = 5
    min_bytes_per_batch = par.MIN_BYTES_PER_BATCH
    max_bytes_per_batch = par.MAX_BYTES_PER_BATCH

    num_rows_batch, num_bytes_batch = _test_restack_batches(
        num_batches=num_batches,
        num_rows_per_batch=num_rows_per_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
    )
    _check_num_rows_batch(
        num_rows_batch=num_rows_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
    )
    _check_num_bytes_batch(
        num_bytes_batch=num_bytes_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
        num_bytes_per_row=num_bytes_per_row,
    )


def test_restack_batches_3():
    num_batches = 1
    num_rows_per_batch = 1
    num_bytes_per_row = generate_record_batch(n=1).nbytes
    min_rows_per_batch = par.MIN_ROWS_PER_BATCH
    max_rows_per_batch = par.MAX_ROWS_PER_BATCH
    min_bytes_per_batch = num_bytes_per_row * 2
    max_bytes_per_batch = num_bytes_per_row * 5

    num_rows_batch, num_bytes_batch = _test_restack_batches(
        num_batches=num_batches,
        num_rows_per_batch=num_rows_per_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
    )
    _check_num_rows_batch(
        num_rows_batch=num_rows_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
    )
    _check_num_bytes_batch(
        num_bytes_batch=num_bytes_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
        num_bytes_per_row=num_bytes_per_row,
    )


def test_restack_batches_4():
    num_batches = 1
    num_rows_per_batch = 11
    num_bytes_per_row = generate_record_batch(n=1).nbytes
    min_rows_per_batch = par.MIN_ROWS_PER_BATCH
    max_rows_per_batch = par.MAX_ROWS_PER_BATCH
    min_bytes_per_batch = num_bytes_per_row * 2
    max_bytes_per_batch = num_bytes_per_row * 5

    num_rows_batch, num_bytes_batch = _test_restack_batches(
        num_batches=num_batches,
        num_rows_per_batch=num_rows_per_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
    )
    _check_num_rows_batch(
        num_rows_batch=num_rows_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
    )
    _check_num_bytes_batch(
        num_bytes_batch=num_bytes_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
        num_bytes_per_row=num_bytes_per_row,
    )


def test_restack_batches_5():
    num_batches = 67
    num_rows_per_batch = 420
    num_bytes_per_row = generate_record_batch(n=1).nbytes
    min_rows_per_batch = 13
    max_rows_per_batch = 666
    min_bytes_per_batch = par.MIN_BYTES_PER_BATCH
    max_bytes_per_batch = par.MAX_BYTES_PER_BATCH

    num_rows_batch, num_bytes_batch = _test_restack_batches(
        num_batches=num_batches,
        num_rows_per_batch=num_rows_per_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
    )
    _check_num_rows_batch(
        num_rows_batch=num_rows_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
    )
    _check_num_bytes_batch(
        num_bytes_batch=num_bytes_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
        num_bytes_per_row=num_bytes_per_row,
    )


def test_restack_batches_6():
    num_batches = 67
    num_rows_per_batch = 420
    num_bytes_per_row = generate_record_batch(n=1).nbytes
    min_rows_per_batch = par.MIN_ROWS_PER_BATCH
    max_rows_per_batch = par.MAX_ROWS_PER_BATCH
    min_bytes_per_batch = 42
    max_bytes_per_batch = 42069

    num_rows_batch, num_bytes_batch = _test_restack_batches(
        num_batches=num_batches,
        num_rows_per_batch=num_rows_per_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
    )
    _check_num_rows_batch(
        num_rows_batch=num_rows_batch,
        min_rows_per_batch=min_rows_per_batch,
        max_rows_per_batch=max_rows_per_batch,
    )
    _check_num_bytes_batch(
        num_bytes_batch=num_bytes_batch,
        min_bytes_per_batch=min_bytes_per_batch,
        max_bytes_per_batch=max_bytes_per_batch,
        num_bytes_per_row=num_bytes_per_row,
    )
