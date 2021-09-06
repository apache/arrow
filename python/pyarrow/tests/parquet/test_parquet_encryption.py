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
import pytest
from datetime import timedelta

import pyarrow as pa
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None


PARQUET_NAME = 'encrypted_table.in_mem.parquet'
FOOTER_KEY = b"0123456789112345"
FOOTER_KEY_NAME = "footer_key"
COL_KEY = b"1234567890123450"
COL_KEY_NAME = "col_key"
BASIC_ENCRYPTION_CONFIG = pq.EncryptionConfiguration(
    footer_key=FOOTER_KEY_NAME,
    column_keys={
        COL_KEY_NAME: ["a", "b"],
    },
)


@pytest.fixture(scope='module')
def data_table():
    data_table = pa.Table.from_pydict({
        'a': pa.array([1, 2, 3]),
        'b': pa.array(['a', 'b', 'c']),
        'c': pa.array(['x', 'y', 'z'])
    })
    return data_table


class InMemoryKmsClient(pq.KmsClient):
    """This is a mock class implementation of KmsClient, built for testing only.
    """

    def __init__(self, config):
        """Create an InMemoryKmsClient instance."""
        pq.KmsClient.__init__(self)
        self.master_keys_map = config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_identifier):
        """Not a secure cipher - the wrapped key
        is just the master key concatenated with key bytes"""
        master_key_bytes = self.master_keys_map[master_key_identifier].encode(
            'utf-8')
        wrapped_key = b"".join([master_key_bytes, key_bytes])
        result = base64.b64encode(wrapped_key)
        return result

    def unwrap_key(self, wrapped_key, master_key_identifier):
        """Not a secure cipher - just extract the key from
        the wrapped key"""
        expected_master_key = self.master_keys_map[master_key_identifier]
        decoded_wrapped_key = base64.b64decode(wrapped_key)
        master_key_bytes = decoded_wrapped_key[:16]
        decrypted_key = decoded_wrapped_key[16:]
        if (expected_master_key == master_key_bytes.decode('utf-8')):
            return decrypted_key
        raise ValueError("Incorrect master key used",
                         master_key_bytes, decrypted_key)


def verify_file_encrypted(path):
    """Verify that the file is encrypted by looking at its first 4 bytes.
    If it's the magic string PARE
    then this is a parquet with encrypted footer."""
    with open(path, "rb") as file:
        magic_str = file.read(4)
        # Verify magic string for parquet with encrypted footer is PARE
        assert(magic_str == b'PARE')


@pytest.mark.parquet
def test_encrypted_parquet_write_read(tempdir, data_table):
    """Write an encrypted parquet, verify it's encrypted, and then read it."""
    path = tempdir / PARQUET_NAME

    # Encrypt the footer with the footer key,
    # encrypt column `a` and column `b` with another key,
    # keep `c` plaintext
    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME,
        column_keys={
            COL_KEY_NAME: ["a", "b"],
        },
        encryption_algorithm="AES_GCM_V1",
        cache_lifetime=timedelta(minutes=5.0),
        data_key_length_bits=256)

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={
            FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
            COL_KEY_NAME: COL_KEY.decode("UTF-8"),
        }
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    # Write with encryption properties
    write_encrypted_parquet(path, data_table, encryption_config,
                            kms_connection_config, crypto_factory)
    verify_file_encrypted(path)

    # Read with decryption properties
    decryption_config = pq.DecryptionConfiguration(
        cache_lifetime=timedelta(minutes=5.0))
    result_table = read_encrypted_parquet(
        path, decryption_config, kms_connection_config, crypto_factory)
    assert data_table.equals(result_table)


def write_encrypted_parquet(path, table, encryption_config,
                            kms_connection_config, crypto_factory):
    file_encryption_properties = crypto_factory.file_encryption_properties(
        kms_connection_config, encryption_config)
    assert(file_encryption_properties is not None)
    with pq.ParquetWriter(
            path, table.schema,
            encryption_properties=file_encryption_properties) as writer:
        writer.write_table(table)


def read_encrypted_parquet(path, decryption_config,
                           kms_connection_config, crypto_factory):
    file_decryption_properties = crypto_factory.file_decryption_properties(
        kms_connection_config, decryption_config)
    assert(file_decryption_properties is not None)
    meta = pq.read_metadata(
        path, decryption_properties=file_decryption_properties)
    assert(meta.num_columns == 3)
    schema = pq.read_schema(
        path, decryption_properties=file_decryption_properties)
    assert(len(schema.names) == 3)

    result = pq.ParquetFile(
        path, decryption_properties=file_decryption_properties)
    return result.read(use_threads=False)


@pytest.mark.parquet
def test_encrypted_parquet_write_read_wrong_key(tempdir, data_table):
    """Write an encrypted parquet, verify it's encrypted,
    and then read it using wrong keys."""
    path = tempdir / PARQUET_NAME

    # Encrypt the footer with the footer key,
    # encrypt column `a` and column `b` with another key,
    # keep `c` plaintext
    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME,
        column_keys={
            COL_KEY_NAME: ["a", "b"],
        },
        encryption_algorithm="AES_GCM_V1",
        cache_lifetime=timedelta(minutes=5.0),
        data_key_length_bits=256)

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={
            FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
            COL_KEY_NAME: COL_KEY.decode("UTF-8"),
        }
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    # Write with encryption properties
    write_encrypted_parquet(path, data_table, encryption_config,
                            kms_connection_config, crypto_factory)
    verify_file_encrypted(path)

    # Read with decryption properties
    wrong_kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={
            # Wrong keys - mixup in names
            FOOTER_KEY_NAME: COL_KEY.decode("UTF-8"),
            COL_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
        }
    )
    decryption_config = pq.DecryptionConfiguration(
        cache_lifetime=timedelta(minutes=5.0))
    with pytest.raises(ValueError, match=r"Incorrect master key used"):
        read_encrypted_parquet(
            path, decryption_config, wrong_kms_connection_config,
            crypto_factory)


@pytest.mark.parquet
def test_encrypted_parquet_read_no_decryption_config(tempdir, data_table):
    """Write an encrypted parquet, verify it's encrypted,
    but then try to read it without decryption properties."""
    test_encrypted_parquet_write_read(tempdir, data_table)
    # Read without decryption properties
    with pytest.raises(IOError, match=r"no decryption"):
        pq.ParquetFile(tempdir / PARQUET_NAME).read()


@pytest.mark.parquet
def test_encrypted_parquet_read_metadata_no_decryption_config(
        tempdir, data_table):
    """Write an encrypted parquet, verify it's encrypted,
    but then try to read its metadata without decryption properties."""
    test_encrypted_parquet_write_read(tempdir, data_table)
    # Read metadata without decryption properties
    with pytest.raises(IOError, match=r"no decryption"):
        pq.read_metadata(tempdir / PARQUET_NAME)


@pytest.mark.parquet
def test_encrypted_parquet_read_schema_no_decryption_config(
        tempdir, data_table):
    """Write an encrypted parquet, verify it's encrypted,
    but then try to read its schema without decryption properties."""
    test_encrypted_parquet_write_read(tempdir, data_table)
    with pytest.raises(IOError, match=r"no decryption"):
        pq.read_schema(tempdir / PARQUET_NAME)


@pytest.mark.parquet
def test_encrypted_parquet_write_no_col_key(tempdir, data_table):
    """Write an encrypted parquet, but give only footer key,
    without column key."""
    path = tempdir / 'encrypted_table_no_col_key.in_mem.parquet'

    # Encrypt the footer with the footer key
    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME)

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={
            FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
            COL_KEY_NAME: COL_KEY.decode("UTF-8"),
        }
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    with pytest.raises(RuntimeError, match=r"column_keys"):
        # Write with encryption properties
        write_encrypted_parquet(path, data_table, encryption_config,
                                kms_connection_config, crypto_factory)


@pytest.mark.parquet
def test_encrypted_parquet_write_kms_error(tempdir, data_table):
    """Write an encrypted parquet, but raise KeyError in KmsClient."""
    path = tempdir / 'encrypted_table_kms_error.in_mem.parquet'
    encryption_config = BASIC_ENCRYPTION_CONFIG

    # Empty master_keys_map
    kms_connection_config = pq.KmsConnectionConfig()

    def kms_factory(kms_connection_configuration):
        # Empty master keys map will cause KeyError to be raised
        # on wrap/unwrap calls
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    with pytest.raises(RuntimeError, match="footer_key.*KeyError"):
        # Write with encryption properties
        write_encrypted_parquet(path, data_table, encryption_config,
                                kms_connection_config, crypto_factory)


@pytest.mark.parquet
def test_encrypted_parquet_write_kms_specific_error(tempdir, data_table):
    """Write an encrypted parquet, but raise KeyError in KmsClient."""
    path = tempdir / 'encrypted_table_kms_error.in_mem.parquet'
    encryption_config = BASIC_ENCRYPTION_CONFIG

    # Empty master_keys_map
    kms_connection_config = pq.KmsConnectionConfig()

    class ThrowingKmsClient(pq.KmsClient):
        """A KmsClient implementation that throws exception in
        wrap/unwrap calls
        """

        def __init__(self, config):
            """Create an InMemoryKmsClient instance."""
            pq.KmsClient.__init__(self)
            self.config = config

        def wrap_key(self, key_bytes, master_key_identifier):
            raise ValueError("Cannot Wrap Key")

        def unwrap_key(self, wrapped_key, master_key_identifier):
            raise ValueError("Cannot Unwrap Key")

    def kms_factory(kms_connection_configuration):
        # Exception thrown in wrap/unwrap calls
        return ThrowingKmsClient(kms_connection_configuration)

    with pytest.raises(RuntimeError, match="Cannot Wrap Key.*ValueError"):
        crypto_factory = pq.CryptoFactory(kms_factory)
        # Write with encryption properties
        write_encrypted_parquet(path, data_table, encryption_config,
                                kms_connection_config, crypto_factory)


@pytest.mark.parquet
def test_encrypted_parquet_write_kms_factory_error(tempdir, data_table):
    """Write an encrypted parquet, but raise ValueError in kms_factory."""
    path = tempdir / 'encrypted_table_kms_factory_error.in_mem.parquet'
    encryption_config = BASIC_ENCRYPTION_CONFIG

    # Empty master_keys_map
    kms_connection_config = pq.KmsConnectionConfig()

    def kms_factory(kms_connection_configuration):
        raise ValueError('Cannot create KmsClient')

    crypto_factory = pq.CryptoFactory(kms_factory)
    with pytest.raises(RuntimeError,
                       match="Cannot create KmsClient.*ValueError"):
        # Write with encryption properties
        write_encrypted_parquet(path, data_table, encryption_config,
                                kms_connection_config, crypto_factory)


@pytest.mark.parquet
def test_encrypted_parquet_write_kms_factory_type_error(tempdir, data_table):
    """Write an encrypted parquet, but use wrong KMS client type
    that doesn't implement KmsClient."""
    path = tempdir / 'encrypted_table_kms_factory_error.in_mem.parquet'
    encryption_config = BASIC_ENCRYPTION_CONFIG

    # Empty master_keys_map
    kms_connection_config = pq.KmsConnectionConfig()

    class WrongTypeKmsClient():
        """This is not an implementation of KmsClient.
        """

        def __init__(self, config):
            self.master_keys_map = config.custom_kms_conf

        def wrap_key(self, key_bytes, master_key_identifier):
            return None

        def unwrap_key(self, wrapped_key, master_key_identifier):
            return None

    def kms_factory(kms_connection_configuration):
        return WrongTypeKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    with pytest.raises(RuntimeError, match=r"TypeError"):
        # Write with encryption properties
        write_encrypted_parquet(path, data_table, encryption_config,
                                kms_connection_config, crypto_factory)


@pytest.mark.parquet
def test_encrypted_parquet_encryption_configuration():
    def validate_encryption_configuration(encryption_config):
        assert(FOOTER_KEY_NAME == encryption_config.footer_key)
        assert(["a", "b"] == encryption_config.column_keys[COL_KEY_NAME])
        assert("AES_GCM_CTR_V1" == encryption_config.encryption_algorithm)
        assert(encryption_config.plaintext_footer)
        assert(not encryption_config.double_wrapping)
        assert(timedelta(minutes=10.0) == encryption_config.cache_lifetime)
        assert(not encryption_config.internal_key_material)
        assert(192 == encryption_config.data_key_length_bits)

    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME,
        column_keys={COL_KEY_NAME: ["a", "b"], },
        encryption_algorithm="AES_GCM_CTR_V1",
        plaintext_footer=True,
        double_wrapping=False,
        cache_lifetime=timedelta(minutes=10.0),
        internal_key_material=False,
        data_key_length_bits=192,
    )
    validate_encryption_configuration(encryption_config)

    encryption_config_1 = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME)
    encryption_config_1.column_keys = {COL_KEY_NAME: ["a", "b"], }
    encryption_config_1.encryption_algorithm = "AES_GCM_CTR_V1"
    encryption_config_1.plaintext_footer = True
    encryption_config_1.double_wrapping = False
    encryption_config_1.cache_lifetime = timedelta(minutes=10.0)
    encryption_config_1.internal_key_material = False
    encryption_config_1.data_key_length_bits = 192
    validate_encryption_configuration(encryption_config_1)


@pytest.mark.parquet
def test_encrypted_parquet_decryption_configuration():
    decryption_config = pq.DecryptionConfiguration(
        cache_lifetime=timedelta(minutes=10.0))
    assert(timedelta(minutes=10.0) == decryption_config.cache_lifetime)

    decryption_config_1 = pq.DecryptionConfiguration()
    decryption_config_1.cache_lifetime = timedelta(minutes=10.0)
    assert(timedelta(minutes=10.0) == decryption_config_1.cache_lifetime)


@pytest.mark.parquet
def test_encrypted_parquet_kms_configuration():
    def validate_kms_connection_config(kms_connection_config):
        assert("Instance1" == kms_connection_config.kms_instance_id)
        assert("URL1" == kms_connection_config.kms_instance_url)
        assert("MyToken" == kms_connection_config.key_access_token)
        assert({"key1": "key_material_1", "key2": "key_material_2"} ==
               kms_connection_config.custom_kms_conf)

    kms_connection_config = pq.KmsConnectionConfig(
        kms_instance_id="Instance1",
        kms_instance_url="URL1",
        key_access_token="MyToken",
        custom_kms_conf={
            "key1": "key_material_1",
            "key2": "key_material_2",
        })
    validate_kms_connection_config(kms_connection_config)

    kms_connection_config_1 = pq.KmsConnectionConfig()
    kms_connection_config_1.kms_instance_id = "Instance1"
    kms_connection_config_1.kms_instance_url = "URL1"
    kms_connection_config_1.key_access_token = "MyToken"
    kms_connection_config_1.custom_kms_conf = {
        "key1": "key_material_1",
        "key2": "key_material_2",
    }
    validate_kms_connection_config(kms_connection_config_1)


@pytest.mark.parquet
def test_encrypted_parquet_write_read_uniform_ctr(tempdir, data_table):
    """Write an encrypted parquet, with uniform encryption
    and GCM_CTR encryption algorithm,
    verify it's encrypted, and then read it."""
    path = tempdir / PARQUET_NAME

    # Encrypt the file with the footer key
    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME,
        column_keys={},
        uniform_encryption=True,
        encryption_algorithm="AES_GCM_CTR_V1")

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8")}
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    # Write with encryption properties
    write_encrypted_parquet(path, data_table, encryption_config,
                            kms_connection_config, crypto_factory)
    verify_file_encrypted(path)

    # Read with decryption properties
    decryption_config = pq.DecryptionConfiguration()
    result_table = read_encrypted_parquet(
        path, decryption_config, kms_connection_config, crypto_factory)
    assert data_table.equals(result_table)


@pytest.mark.parquet
@pytest.mark.xfail(reason="Plaintext footer - reading plaintext column subset"
                   " reads encrypted columns too")
def test_encrypted_parquet_write_read_plain_footer_single_wrapping(
        tempdir, data_table):
    """Write an encrypted parquet, with plaintext footer
    and with single wrapping,
    verify it's encrypted, and then read plaintext columns."""
    path = tempdir / PARQUET_NAME

    # Encrypt the footer with the footer key,
    # encrypt column `a` and column `b` with another key,
    # keep `c` plaintext
    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME,
        column_keys={
            COL_KEY_NAME: ["a", "b"],
        },
        plaintext_footer=True,
        double_wrapping=False)

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={
            FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
            COL_KEY_NAME: COL_KEY.decode("UTF-8"),
        }
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    # Write with encryption properties
    write_encrypted_parquet(path, data_table, encryption_config,
                            kms_connection_config, crypto_factory)

    # # Read without decryption properties only the plaintext column
    # result = pq.ParquetFile(path)
    # result_table = result.read(columns='c', use_threads=False)
    # assert table.num_rows == result_table.num_rows


@pytest.mark.parquet
@pytest.mark.xfail(reason="External key material not supported yet")
def test_encrypted_parquet_write_external(tempdir, data_table):
    """Write an encrypted parquet, with external key
    material.
    Currently it's not implemented, so should throw
    an exception"""
    path = tempdir / PARQUET_NAME

    # Encrypt the file with the footer key
    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME,
        column_keys={},
        uniform_encryption=True,
        internal_key_material=False)

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8")}
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    # Write with encryption properties
    write_encrypted_parquet(path, data_table, encryption_config,
                            kms_connection_config, crypto_factory)


@pytest.mark.parquet
@pytest.mark.skip(reason="Multithreaded read sometimes fails decryption"
                  " finalization and sometimes with Segmentation fault")
def test_encrypted_parquet_loop(tempdir, data_table):
    """Write an encrypted parquet, verify it's encrypted,
    and then read it multithreaded in a loop."""
    path = tempdir / PARQUET_NAME

    # Encrypt the footer with the footer key,
    # encrypt column `a` and column `b` with another key,
    # keep `c` plaintext
    encryption_config = BASIC_ENCRYPTION_CONFIG

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={
            FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
            COL_KEY_NAME: COL_KEY.decode("UTF-8"),
        }
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)

    # Write with encryption properties
    write_encrypted_parquet(path, data_table, encryption_config,
                            kms_connection_config, crypto_factory)
    verify_file_encrypted(path)

    decryption_config = pq.DecryptionConfiguration(
        cache_lifetime=timedelta(minutes=5.0))

    for i in range(50):
        # Read with decryption properties
        file_decryption_properties = crypto_factory.file_decryption_properties(
            kms_connection_config, decryption_config)
        assert(file_decryption_properties is not None)

        result = pq.ParquetFile(
            path, decryption_properties=file_decryption_properties)
        result_table = result.read(use_threads=True)
        assert data_table.equals(result_table)
