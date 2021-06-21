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
import pyarrow as pa
import pytest

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

import base64
from Cryptodome.Cipher import AES
from collections import OrderedDict
from datetime import timedelta

DATA_TABLE = pa.Table.from_pydict(
    OrderedDict([
        ('a', pa.array([1, 2, 3])),
        ('b', pa.array(['a', 'b', 'c'])),
        ('c', pa.array(['x', 'y', 'z']))
    ])
)
PARQUET_NAME = 'encrypted_table.in_mem.parquet'
FOOTER_KEY = "0123456789ABCDEF"
FOOTER_KEY_NAME = "footer_key"
COL_KEY = "1234123412341234"
COL_KEY_NAME = "col_key"
BASIC_ENCRYPTION_CONFIG = pq.EncryptionConfiguration(
    footer_key=FOOTER_KEY_NAME,
    column_keys={
        COL_KEY_NAME: ["a", "b"],
    },
)

class InMemoryKmsClient(pq.KmsClient):
    """This is a mock class implementation of KmsClient, built for testing only.
    """

    def __init__(self, config):
        """Create an InMemoryKmsClient instance."""
        pq.KmsClient.__init__(self)
        self.master_keys_map = config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_identifier):
        """Wrap key key_bytes with key identified by master_key_identifier.
        The result contains nonce concatenated before the encrypted key."""
        master_key = self.master_keys_map[master_key_identifier]
        # Create a cipher object to encrypt data
        cipher = AES.new(master_key.encode('utf-8'), AES.MODE_GCM)
        nonce = cipher.nonce
        encrypted_key = cipher.encrypt(key_bytes)
        result = base64.b64encode(nonce + encrypted_key)
        return result

    def unwrap_key(self, wrapped_key, master_key_identifier):
        """Unwrap wrapped_key with key identified by master_key_identifier"""
        master_key = self.master_keys_map[master_key_identifier]
        decoded_wrapped_key = base64.b64decode(wrapped_key)
        nonce, encrypted_key = \
            decoded_wrapped_key[:16], decoded_wrapped_key[-16:]
        # Create a cipher object to decrypt data
        cipher = AES.new(master_key.encode('utf-8'), AES.MODE_GCM, nonce)
        decrypted_key = cipher.decrypt(encrypted_key)
        return decrypted_key


def verify_file_encrypted(path):
    """Verify that the file is encrypted by looking at its first 4 bytes.
    If it's the magic string PARE
    then this is a parquet with encrypted footer."""
    with open(path, "rb") as file:
        magic_str = file.read(4)
        # Verify magic string for parquet with encrypted footer is PARE
        assert(magic_str == b'PARE')


@pytest.mark.parquet
def test_encrypted_parquet_write_read(tempdir):
    """Write an encrypted parquet, verify it's encrypted, and then read it."""
    path = tempdir / PARQUET_NAME
    table = DATA_TABLE

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
            FOOTER_KEY_NAME: FOOTER_KEY,
            COL_KEY_NAME: COL_KEY,
        }
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pq.CryptoFactory(kms_factory)
    # Write with encryption properties
    write_encrypted_parquet(path, table, encryption_config,
                            kms_connection_config, crypto_factory)
    verify_file_encrypted(path)

    # Read with decryption properties
    decryption_config = pq.DecryptionConfiguration(
        cache_lifetime=timedelta(minutes=5.0))
    result_table = read_encrypted_parquet(
        path, decryption_config, kms_connection_config, crypto_factory)
    assert table.equals(result_table)


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
    return result.read()


@pytest.mark.parquet
def test_encrypted_parquet_read_no_decryption_config(tempdir):
    """Write an encrypted parquet, verify it's encrypted,
    but then try to read it without decryption properties."""
    with pytest.raises(IOError, match=r"no decryption"):
        test_encrypted_parquet_write_read(tempdir)
        path = tempdir / PARQUET_NAME
        result = pq.ParquetFile(path)
        assert(result is not None)


@pytest.mark.parquet
def test_encrypted_parquet_read_metadata_no_decryption_config(tempdir):
    """Write an encrypted parquet, verify it's encrypted,
    but then try to read its metadata without decryption properties."""
    with pytest.raises(IOError, match=r"no decryption"):
        test_encrypted_parquet_write_read(tempdir)
        path = tempdir / PARQUET_NAME
        meta = pq.read_metadata(path)
        assert(meta is not None)


@pytest.mark.parquet
def test_encrypted_parquet_read_schema_no_decryption_config(tempdir):
    """Write an encrypted parquet, verify it's encrypted,
    but then try to read its schema without decryption properties."""
    with pytest.raises(IOError, match=r"no decryption"):
        test_encrypted_parquet_write_read(tempdir)
        path = tempdir / PARQUET_NAME
        schema = pq.read_schema(path)
        assert(schema is not None)


@pytest.mark.parquet
def test_encrypted_parquet_write_no_col_key(tempdir):
    """Write an encrypted parquet, but give only footer key,
    without column key."""
    path = tempdir / 'encrypted_table_no_col_key.in_mem.parquet'
    table = DATA_TABLE

    # Encrypt the footer with the footer key
    encryption_config = pq.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME)

    kms_connection_config = pq.KmsConnectionConfig(
        custom_kms_conf={
            FOOTER_KEY_NAME: FOOTER_KEY,
            COL_KEY_NAME: COL_KEY,
        }
    )

    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    with pytest.raises(RuntimeError, match=r"column_keys"):
        crypto_factory = pq.CryptoFactory(kms_factory)
        # Write with encryption properties
        write_encrypted_parquet(path, table, encryption_config,
                                kms_connection_config, crypto_factory)


@pytest.mark.parquet
def test_encrypted_parquet_write_kms_error(tempdir):
    """Write an encrypted parquet, but raise KeyError in KmsClient."""
    path = tempdir / 'encrypted_table_kms_error.in_mem.parquet'
    table = DATA_TABLE

    encryption_config = BASIC_ENCRYPTION_CONFIG

    # Empty master_keys_map
    kms_connection_config = pq.KmsConnectionConfig()

    def kms_factory(kms_connection_configuration):
        # Empty master keys map will cause KeyError to be raised
        # on wrap/unwrap calls
        return InMemoryKmsClient(kms_connection_configuration)

    with pytest.raises(KeyError):
        crypto_factory = pq.CryptoFactory(kms_factory)
        # Write with encryption properties
        write_encrypted_parquet(path, table, encryption_config,
                                kms_connection_config, crypto_factory)

@pytest.mark.parquet
def test_encrypted_parquet_write_kms_factory_error(tempdir):
    """Write an encrypted parquet, but raise ValueError in kms_factory."""
    path = tempdir / 'encrypted_table_kms_factory_error.in_mem.parquet'
    table = DATA_TABLE

    encryption_config = BASIC_ENCRYPTION_CONFIG

    # Empty master_keys_map
    kms_connection_config = pq.KmsConnectionConfig()

    def kms_factory(kms_connection_configuration):
        raise ValueError('I am a value erorr')

    with pytest.raises(ValueError):
        crypto_factory = pq.CryptoFactory(kms_factory)
        # Write with encryption properties
        write_encrypted_parquet(path, table, encryption_config,
                                kms_connection_config, crypto_factory)

@pytest.mark.parquet
def test_encrypted_parquet_write_kms_factory_type_error(tempdir):
    """Write an encrypted parquet, but use wrong KMS client type
    that doesn't implement KmsClient."""
    path = tempdir / 'encrypted_table_kms_factory_error.in_mem.parquet'
    table = DATA_TABLE

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

    with pytest.raises(TypeError):
        crypto_factory = pq.CryptoFactory(kms_factory)
        # Write with encryption properties
        write_encrypted_parquet(path, table, encryption_config,
                                kms_connection_config, crypto_factory)

@pytest.mark.parquet
def test_encrypted_parquet_loop(tempdir):
    for i in range(50):
        test_encrypted_parquet_write_read(tempdir)
