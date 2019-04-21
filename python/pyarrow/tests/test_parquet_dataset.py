
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq


def get_props(obj):
    """Collect properties of object in a dictionary.
    """
    import inspect
    d = {}
    for n in dir(obj):
        if n.startswith('_'):
            continue
        a = inspect.getattr_static(obj, n)
        if inspect.isdatadescriptor(a):
            try:
                v = getattr(obj, n)
            except Exception as msg:
                v = str(msg)
            if n == 'statistics':
                v = get_props(v)
            elif n == 'schema':
                v = v.to_arrow_schema()
            d[n] = v
    return d


def test_dataset_metadata(tempdir):
    path = tempdir / "ARROW-1983-dataset"

    # create and write a test dataset
    df = pd.DataFrame({
        'one': [1, 2, 3],
        'two': [-1, -2, -3],
        'three': [[1, 2], [2, 3], [3, 4]],
        })
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path=str(path),
                        write_metadata=True,
                        partition_cols=['one', 'two'])

    # open the dataset and collect metadata from pieces:
    dataset = pq.ParquetDataset(path)
    metadata_list = [p.get_metadata() for p in dataset.pieces]

    # read dataset metadata only:
    md_path = str(path) + "_metadata.parquet"
    metadata_list2 = pq.read_metadata_list(md_path)

    # compare metadata list content:
    for md, md2 in zip(metadata_list, metadata_list2):
        assert get_props(md) == get_props(md2)
        for r in range(md.num_row_groups):
            rg = md.row_group(r)
            rg2 = md2.row_group(r)
            assert get_props(rg) == get_props(rg2)
            for c in range(rg.num_columns):
                col = rg.column(c)
                col2 = rg2.column(c)
                assert get_props(col) == get_props(col2)

    path = tempdir / "ARROW-1983-single.parquet"
    pq.write_table(table, str(path))

    # make sure read_metadata_list works on a single file
    metadata_list3 = pq.read_metadata_list(path)
    assert len(metadata_list3) == 1
