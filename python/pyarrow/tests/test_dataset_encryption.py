# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import base64
from datetime import timedelta
import random
import pyarrow.fs as fs
import pyarrow as pa

import pytest

encryption_unavailable = False

try:
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
except ImportError:
    pq = None
    ds = None

try:
    from pyarrow.tests.parquet.encryption import InMemoryKmsClient
    import pyarrow.parquet.encryption as pe
except ImportError:
    encryption_unavailable = True


# Marks all of the tests in this module
pytestmark = pytest.mark.dataset


FOOTER_KEY = b"0123456789112345"
FOOTER_KEY_NAME = "footer_key"
COL_KEY = b"1234567890123450"
COL_KEY_NAME = "col_key"


def create_sample_table():
    return pa.table(
        {
            "year": [2020, 2022, 2021, 2022, 2019, 2021],
            "n_legs": [2, 2, 4, 4, 5, 100],
            "animal": [
                "Flamingo",
                "Parrot",
                "Dog",
                "Horse",
                "Brittle stars",
                "Centipede",
            ],
        }
    )


def create_encryption_config(footer_key, column_keys):
    return pe.EncryptionConfiguration(
        footer_key=footer_key,
        plaintext_footer=False,
        column_keys=column_keys,
        encryption_algorithm="AES_GCM_V1",
        # requires timedelta or an assertion is raised
        cache_lifetime=timedelta(minutes=5.0),
        data_key_length_bits=256,
    )


def create_decryption_config():
    return pe.DecryptionConfiguration(cache_lifetime=300)


def create_kms_connection_config(keys):
    return pe.KmsConnectionConfig(
        custom_kms_conf={
            key_name: key.decode("UTF-8") if isinstance(key, bytes) else key
            for key_name, key in keys.items()
        }
    )


def kms_factory(kms_connection_configuration):
    return InMemoryKmsClient(kms_connection_configuration)


def do_test_dataset_encryption_decryption(
    table,
    footer_key=FOOTER_KEY_NAME,
    column_keys={COL_KEY_NAME: ["n_legs", "animal"]},
    keys={FOOTER_KEY_NAME: FOOTER_KEY, COL_KEY_NAME: COL_KEY}
):
    encryption_config = create_encryption_config(footer_key, column_keys)
    decryption_config = create_decryption_config()
    kms_connection_config = create_kms_connection_config(keys)

    crypto_factory = pe.CryptoFactory(kms_factory)
    parquet_encryption_cfg = ds.ParquetEncryptionConfig(
        crypto_factory, kms_connection_config, encryption_config
    )
    parquet_decryption_cfg = ds.ParquetDecryptionConfig(
        crypto_factory, kms_connection_config, decryption_config
    )

    # create write_options with dataset encryption config
    pformat = pa.dataset.ParquetFileFormat()
    write_options = pformat.make_write_options(encryption_config=parquet_encryption_cfg)

    mockfs = fs._MockFileSystem()
    mockfs.create_dir("/")

    ds.write_dataset(
        data=table,
        base_dir="sample_dataset",
        format=pformat,
        file_options=write_options,
        filesystem=mockfs,
    )

    # read without decryption config -> should error if dataset was properly encrypted
    pformat = pa.dataset.ParquetFileFormat()
    with pytest.raises(IOError, match=r"no decryption"):
        ds.dataset("sample_dataset", format=pformat, filesystem=mockfs)

    # helper method for following tests
    def create_format_with_keys(keys):
        kms_connection_config = create_kms_connection_config(keys)
        parquet_decryption_cfg = ds.ParquetDecryptionConfig(
            crypto_factory, kms_connection_config, decryption_config
        )
        pq_scan_opts = ds.ParquetFragmentScanOptions(
            decryption_config=parquet_decryption_cfg
        )
        return pa.dataset.ParquetFileFormat(default_fragment_scan_options=pq_scan_opts)

    def assert_read_table_with_keys_success(keys, column_names):
        pformat = create_format_with_keys(keys)
        dataset = ds.dataset("sample_dataset", format=pformat, filesystem=mockfs)
        assert table.select(column_names).equals(dataset.to_table(columns=column_names))

    def assert_read_table_with_keys_failure(keys, column_names):
        pformat = create_format_with_keys(keys)
        # creating the dataset works
        dataset = ds.dataset("sample_dataset", format=pformat, filesystem=mockfs)
        with pytest.raises(KeyError, match=r"col_key"):
            # reading those columns fails
            _ = dataset.to_table(column_names)

    # some notable column names and keys
    all_column_names = table.column_names
    encrypted_column_names = [column_name
                              for key_name, column_names in column_keys.items()
                              for column_name in column_names]
    plaintext_column_names = [column_name
                              for column_name in all_column_names
                              if column_name not in encrypted_column_names]
    assert len(encrypted_column_names) > 0
    assert len(plaintext_column_names) > 0
    footer_key_only = {FOOTER_KEY_NAME: FOOTER_KEY}
    column_keys_only = {key_name: key for key_name, key in keys.items() if key_name != FOOTER_KEY_NAME}

    # read with footer key only
    assert_read_table_with_keys_success(footer_key_only, plaintext_column_names)
    #assert_read_table_with_keys_failure(footer_key_only, encrypted_column_names)
    #assert_read_table_with_keys_failure(footer_key_only, all_column_names)

    # read with all but footer key
    if len(keys) > 1:
        assert_read_table_with_keys_success(column_keys_only, plaintext_column_names)  # TODO: this is wrong!
        assert_read_table_with_keys_failure(column_keys_only, encrypted_column_names)
        assert_read_table_with_keys_failure(column_keys_only, all_column_names)

        # with footer key and one column key, all plaintext and
        # those encrypted columns that use that key, can be read
        if len(keys) > 2:
            for column_key_name, encrypted_column_names in column_keys.items():
                for encrypted_column_name in encrypted_column_names:
                    footer_key_and_one_column_key = {key_name: key for key_name, key in keys.items()
                                                     if key_name in [FOOTER_KEY_NAME, column_key_name]}
                    assert_read_table_with_keys_success(footer_key_and_one_column_key, plaintext_column_names)
                    assert_read_table_with_keys_success(footer_key_and_one_column_key, plaintext_column_names + [encrypted_column_name])
                    assert_read_table_with_keys_failure(footer_key_and_one_column_key, encrypted_column_names)
                    assert_read_table_with_keys_failure(footer_key_and_one_column_key, all_column_names)

        # with all column keys, all columns can be read
        assert_read_table_with_keys_success(keys, plaintext_column_names)
        assert_read_table_with_keys_failure(keys, encrypted_column_names)  # TODO: this is wrong!
        assert_read_table_with_keys_failure(keys, all_column_names)

    # no matter how many keys are configured, test that whole table can be read
    pq_scan_opts = ds.ParquetFragmentScanOptions(
        decryption_config=parquet_decryption_cfg
    )
    pformat = pa.dataset.ParquetFileFormat(default_fragment_scan_options=pq_scan_opts)
    dataset = ds.dataset("sample_dataset", format=pformat, filesystem=mockfs)

    assert table.equals(dataset.to_table())

    # set decryption properties for parquet fragment scan options
    decryption_properties = crypto_factory.file_decryption_properties(
        kms_connection_config, decryption_config)
    pq_scan_opts = ds.ParquetFragmentScanOptions(
        decryption_properties=decryption_properties
    )

    pformat = pa.dataset.ParquetFileFormat(default_fragment_scan_options=pq_scan_opts)
    dataset = ds.dataset("sample_dataset", format=pformat, filesystem=mockfs)

    assert table.equals(dataset.to_table())


@pytest.mark.skipif(
    encryption_unavailable, reason="Parquet Encryption is not currently enabled"
)
def test_dataset_encryption_decryption():
    do_test_dataset_encryption_decryption(create_sample_table())


@pytest.mark.skipif(
    encryption_unavailable, reason="Parquet Encryption is not currently enabled"
)
@pytest.mark.parametrize("column_name", ["list", "list.list.element"])
def test_list_encryption_decryption(column_name):
    list_data = pa.array(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9], [-1], [-2], [-3]],
        type=pa.list_(pa.int32()),
    )
    table = create_sample_table().append_column("list", list_data)

    column_keys = {COL_KEY_NAME: ["animal", column_name]}
    do_test_dataset_encryption_decryption(table, column_keys=column_keys)


@pytest.mark.skipif(
    encryption_unavailable,
    reason="Parquet Encryption is not currently enabled"
)
@pytest.mark.parametrize(
    "column_name",
    ["map", "map.key", "map.value", "map.key_value.key", "map.key_value.value"]
)
def test_map_encryption_decryption(column_name):
    map_type = pa.map_(pa.string(), pa.int32())
    map_data = pa.array(
        [
            [("k1", 1), ("k2", 2)], [("k1", 3), ("k3", 4)], [("k2", 5), ("k3", 6)],
            [("k4", 7)], [], []
        ],
        type=map_type
    )
    table = create_sample_table().append_column("map", map_data)

    column_keys = {COL_KEY_NAME: ["animal", column_name]}
    do_test_dataset_encryption_decryption(table, column_keys=column_keys)


@pytest.mark.skipif(
    encryption_unavailable, reason="Parquet Encryption is not currently enabled"
)
@pytest.mark.parametrize("column_name", ["struct", "struct.f1", "struct.f2"])
def test_struct_encryption_decryption(column_name):
    struct_fields = [("f1", pa.int32()), ("f2", pa.string())]
    struct_type = pa.struct(struct_fields)
    struct_data = pa.array(
        [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"), (6, "six")],
        type=struct_type
    )
    table = create_sample_table().append_column("struct", struct_data)

    column_keys = {COL_KEY_NAME: ["animal", column_name]}
    do_test_dataset_encryption_decryption(table, column_keys=column_keys)


@pytest.mark.skipif(
    encryption_unavailable,
    reason="Parquet Encryption is not currently enabled"
)
@pytest.mark.parametrize(
    "column_name",
    [
        "col",
        "col.list.element",
        "col.list.element.key_value.key",
        "col.list.element.key_value.value",
        "col.list.element.key_value.value.f1",
        "col.list.element.key_value.value.f2"
    ]
)
def test_deep_nested_encryption_decryption(column_name):
    struct_fields = [("f1", pa.int32()), ("f2", pa.string())]
    struct_type = pa.struct(struct_fields)
    struct1 = (1, "one")
    struct2 = (2, "two")
    struct3 = (3, "three")
    struct4 = (4, "four")
    struct5 = (5, "five")
    struct6 = (6, "six")

    map_type = pa.map_(pa.int32(), struct_type)
    map1 = {1: struct1, 2: struct2}
    map2 = {3: struct3}
    map3 = {4: struct4}
    map4 = {5: struct5, 6: struct6}

    list_type = pa.list_(map_type)
    list1 = [map1, map2]
    list2 = [map3]
    list3 = [map4]
    list_data = [pa.array([list1, list2, None, list3, None, None], type=list_type)]
    table = create_sample_table().append_column("col", list_data)

    column_keys = {COL_KEY_NAME: ["animal", column_name]}
    do_test_dataset_encryption_decryption(table, column_keys=column_keys)


@pytest.mark.skipif(
    not encryption_unavailable, reason="Parquet Encryption is currently enabled"
)
def test_write_dataset_parquet_without_encryption():
    """Test write_dataset with ParquetFileFormat and test if an exception is thrown
    if you try to set encryption_config using make_write_options"""

    # Set the encryption configuration using ParquetFileFormat
    # and make_write_options
    pformat = pa.dataset.ParquetFileFormat()

    with pytest.raises(NotImplementedError):
        _ = pformat.make_write_options(encryption_config="some value")


@pytest.mark.skipif(
    encryption_unavailable, reason="Parquet Encryption is not currently enabled"
)
def test_large_row_encryption_decryption():
    """Test encryption and decryption of a large number of rows."""

    class NoOpKmsClient(pe.KmsClient):
        def wrap_key(self, key_bytes: bytes, _: str) -> bytes:
            b = base64.b64encode(key_bytes)
            return b

        def unwrap_key(self, wrapped_key: bytes, _: str) -> bytes:
            b = base64.b64decode(wrapped_key)
            return b

    row_count = 2**15 + 1
    table = pa.Table.from_arrays(
        [pa.array(
            [random.random() for _ in range(row_count)],
            type=pa.float32()
        )], names=["foo"]
    )

    kms_config = pe.KmsConnectionConfig()
    crypto_factory = pe.CryptoFactory(lambda _: NoOpKmsClient())
    encryption_config = pe.EncryptionConfiguration(
        footer_key="UNIMPORTANT_KEY",
        column_keys={"UNIMPORTANT_KEY": ["foo"]},
        double_wrapping=True,
        plaintext_footer=False,
        data_key_length_bits=128,
    )
    pqe_config = ds.ParquetEncryptionConfig(
        crypto_factory, kms_config, encryption_config
    )
    pqd_config = ds.ParquetDecryptionConfig(
        crypto_factory, kms_config, pe.DecryptionConfiguration()
    )
    scan_options = ds.ParquetFragmentScanOptions(decryption_config=pqd_config)
    file_format = ds.ParquetFileFormat(default_fragment_scan_options=scan_options)
    write_options = file_format.make_write_options(encryption_config=pqe_config)
    file_decryption_properties = crypto_factory.file_decryption_properties(kms_config)

    mockfs = fs._MockFileSystem()
    mockfs.create_dir("/")

    path = "large-row-test-dataset"
    ds.write_dataset(table, path, format=file_format,
                     file_options=write_options, filesystem=mockfs)

    file_path = path + "/part-0.parquet"
    new_table = pq.ParquetFile(
        file_path, decryption_properties=file_decryption_properties,
        filesystem=mockfs
    ).read()
    assert table == new_table

    dataset = ds.dataset(path, format=file_format, filesystem=mockfs)
    new_table = dataset.to_table()
    assert table == new_table
