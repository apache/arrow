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
import datetime
import pyarrow
import pyarrow.parquet as pp
import pyarrow.parquet.encryption as ppe
import pytest
import re


class FooKmsClient(ppe.KmsClient):

    def __init__(self, kms_connection_config):
        ppe.KmsClient.__init__(self)
        self.master_keys_map = kms_connection_config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_identifier):
        master_key_bytes = self.master_keys_map[master_key_identifier].encode('utf-8')
        joint_key = b"".join([master_key_bytes, key_bytes])
        return base64.b64encode(joint_key)

    def unwrap_key(self, wrapped_key, master_key_identifier):
        expected_master = self.master_keys_map[master_key_identifier]
        decoded_key = base64.b64decode(wrapped_key)
        master_key_bytes = decoded_key[:16]
        decrypted_key = decoded_key[16:]
        if (expected_master == master_key_bytes.decode('utf-8')):
            return decrypted_key
        raise ValueError(
            f"Bad master key used [{master_key_bytes}] - [{decrypted_key}]")


def get_data_table():
    sample_data = {
        "orderId": [1001, 1002, 1003],
        "productId": [152, 268, 6548],
        "price": [3.25, 6.48, 2.12],
        "vat": [0.0, 0.2, 0.05]
    }
    return pyarrow.Table.from_pydict(sample_data)


def kms_client_factory(kms_connection_config):
    return FooKmsClient(kms_connection_config)


def get_kms_connection_config():
    return ppe.KmsConnectionConfig(
        custom_kms_conf={
            "footer_key": "012footer_secret",
            "orderid_key": "column_secret001",
            "productid_key": "column_secret002"
        }
    )


def get_encryption_config():
    return ppe.EncryptionConfiguration(
        footer_key="footer_key",
        column_keys={
            "orderid_key": ["orderId"],
            "productid_key": ["productId"]
        },
        encryption_algorithm="AES_GCM_V1",
        cache_lifetime=datetime.timedelta(minutes=2.0),
        data_key_length_bits=128,
        plaintext_footer=True
    )


def get_encryption_properties():
    encryption_config = get_encryption_config()
    crypto_factory = ppe.CryptoFactory(kms_client_factory)
    return crypto_factory.file_encryption_properties(
        get_kms_connection_config(), encryption_config)


def get_external_encryption_config(plaintext_footer=True):
    return ppe.ExternalEncryptionConfiguration(
        footer_key="footer_key",
        column_keys={
            "productid_key": ["productId"]
        },
        encryption_algorithm="AES_GCM_V1",
        cache_lifetime=datetime.timedelta(minutes=2.0),
        data_key_length_bits=128,
        plaintext_footer=plaintext_footer,
        per_column_encryption={
            "orderId": {
                "encryption_algorithm": "AES_GCM_CTR_V1",
                "encryption_key": "orderid_key"
            },
        },
        app_context={
            "user_id": "Picard1701",
            "location": "Presidio"
        },
        connection_config={
            "EXTERNAL_DBPA_V1": {
                "config_file": "path/to/config/file",
                "config_file_decryption_key": "some_key"
            }
        }
    )


def get_external_encryption_properties():
    encryption_config = get_external_encryption_config()
    crypto_factory = ppe.CryptoFactory(kms_client_factory)
    return crypto_factory.external_file_encryption_properties(
        get_kms_connection_config(), encryption_config)


def get_decryption_config():
    return ppe.DecryptionConfiguration(cache_lifetime=datetime.timedelta(minutes=10.0))


def get_decryption_properties():
    decryption_config = get_decryption_config()
    crypto_factory = ppe.CryptoFactory(kms_client_factory)
    return crypto_factory.file_decryption_properties(
        get_kms_connection_config(), decryption_config)


def get_external_decryption_config():
    return ppe.ExternalDecryptionConfiguration(
        cache_lifetime=datetime.timedelta(minutes=10.0),
        app_context={
            "user_id": "Picard1701",
            "location": "Presidio"
        },
        connection_config={
            "EXTERNAL_DBPA_V1": {
                "config_file": "path/to/config/file",
                "config_file_decryption_key": "some_key"
            }
        }
    )


def get_external_decryption_properties():
    decryption_config = get_external_decryption_config()
    crypto_factory = ppe.CryptoFactory(kms_client_factory)
    return crypto_factory.external_file_decryption_properties(
        get_kms_connection_config(), decryption_config)


def write_parquet(table, location, encryption_properties):
    writer = pp.ParquetWriter(
        location,
        table.schema,
        encryption_properties=encryption_properties)
    writer.write_table(table)


def read_parquet(location, decryption_properties):
    reader = pp.ParquetFile(location, decryption_properties=decryption_properties)
    return reader.read()


def test_encryption_configuration_properties():
    """Test the standard EncryptionConfiguration properties to avoid regressions."""

    config = ppe.EncryptionConfiguration(
        footer_key="footer-key-name",
        column_keys={
            "key_1": ["a"],
        },
        encryption_algorithm="EXTERNAL_DBPA_V1",
        plaintext_footer=True,
        double_wrapping=True,
        cache_lifetime=datetime.timedelta(minutes=5.0),
        internal_key_material=True,
        data_key_length_bits=256
    )

    assert isinstance(config, ppe.EncryptionConfiguration)

    assert config.footer_key == "footer-key-name"
    assert config.column_keys == {
        "key_1": ["a"]
    }
    assert config.encryption_algorithm == "EXTERNAL_DBPA_V1"
    assert config.plaintext_footer is True
    assert config.double_wrapping is True
    assert config.cache_lifetime == datetime.timedelta(minutes=5.0)
    assert config.internal_key_material is True
    assert config.data_key_length_bits == 256


def test_external_encryption_configuration_properties():
    """Test the ExternalEncryptionConfig including external-specific fields."""

    external_encryption_config = get_external_encryption_config()
    assert isinstance(external_encryption_config, ppe.ExternalEncryptionConfiguration)

    assert external_encryption_config.footer_key == "footer_key"
    assert external_encryption_config.column_keys == {
        "productid_key": ["productId"]
    }
    assert external_encryption_config.encryption_algorithm == "AES_GCM_V1"
    assert external_encryption_config.plaintext_footer is True
    assert external_encryption_config.double_wrapping is True
    assert external_encryption_config.cache_lifetime == datetime.timedelta(minutes=2.0)
    assert external_encryption_config.internal_key_material is True
    assert external_encryption_config.data_key_length_bits == 128

    assert external_encryption_config.app_context == {
        "user_id": "Picard1701",
        "location": "Presidio"
    }

    assert external_encryption_config.connection_config == {
        "EXTERNAL_DBPA_V1": {
            "config_file": "path/to/config/file",
            "config_file_decryption_key": "some_key"
        }
    }

    assert external_encryption_config.per_column_encryption == {
        "orderId": {
            "encryption_algorithm": "AES_GCM_CTR_V1",
            "encryption_key": "orderid_key"
        },
    }


def test_external_encryption_app_context_invalid_json():
    """Ensure app_context raises TypeError for non-JSON-serializable input."""
    with pytest.raises(
        TypeError,
        match="Failed to serialize app_context: {'invalid': {1, 2, 3}}"
    ):
        ppe.ExternalEncryptionConfiguration(
            footer_key="key",
            app_context={"invalid": set([1, 2, 3])}  # sets are not JSON-serializable
        )


def test_external_encryption_per_column_encryption_invalid_algorithm():
    """Ensure invalid encryption_algorithm raises a ValueError or is rejected."""

    with pytest.raises(ValueError, match="Invalid cipher name: INVALID"):
        ppe.ExternalEncryptionConfiguration(
            footer_key="key",
            per_column_encryption={
                "a": {
                    "encryption_algorithm": "INVALID",
                    "encryption_key": "some_key"
                }
            }
        )


def test_external_encryption_per_column_encryption_new_algorithm():
    """Ensure new encryption_algorithm is accepted."""

    ppe.ExternalEncryptionConfiguration(
        footer_key="key",
        per_column_encryption={
            "a": {
                "encryption_algorithm": "EXTERNAL_DBPA_V1",
                "encryption_key": "key_2"
            }
        }
    )


def test_external_encryption_connection_config_invalid_types():
    """Ensure connection_config rejects non-string keys or values."""
    with pytest.raises(
            TypeError,
            match="All inner config keys/values must be str"):
        config = ppe.ExternalEncryptionConfiguration(
            footer_key="key"
        )
        config.connection_config = {
            "EXTERNAL_DBPA_V1": {
                "config_file": "path/to/file",
                123: "should-fail"  # Invalid: key is not a string
            }
        }

    with pytest.raises(
            TypeError,
            match="All inner config keys/values must be str"):
        config = ppe.ExternalEncryptionConfiguration(
            footer_key="key"
        )
        config.connection_config = {
            "EXTERNAL_DBPA_V1": {
                "config_file": ["not", "a", "string"]  # Invalid: value is not a string
            }
        }


def test_external_encryption_rejects_none_values():
    """Ensure None values are rejected."""
    config = ppe.ExternalEncryptionConfiguration(footer_key="key")

    # per_column_encryption: expect ValueError
    with pytest.raises(TypeError, match="per_column_encryption cannot be None"):
        config.per_column_encryption = None

    # app_context: expect ValueError due to None not being JSON-serializable
    with pytest.raises(ValueError, match="app_context must be JSON-serializable"):
        config.app_context = None

    # connection_config: expect ValueError due to None not being iterable
    with pytest.raises(ValueError, match="Connection config value cannot be None"):
        config.connection_config = None


def test_external_file_encryption_properties_rejects_column_in_two_places():
    """Ensure a column cannot be defined in both column_keys
    and per_column_encryption."""
    config = ppe.ExternalEncryptionConfiguration(
        footer_key="footer_key",
        column_keys={"orderid_key": ["a"]},
        per_column_encryption={"a": {
            "encryption_algorithm": "AES_GCM_V1",
            "encryption_key": "key_2"
        }},
    )
    factory = ppe.CryptoFactory(kms_client_factory)
    with pytest.raises(
        OSError,
        match=re.escape("Multiple keys defined for column [a]")
    ):
        factory.external_file_encryption_properties(get_kms_connection_config(), config)


def test_external_file_encryption_properties_valid():
    """Check class name and module because
    ExternalFileEncryptionProperties is not visible."""
    external_encryption_properties = get_external_encryption_properties()

    assert (
        external_encryption_properties.__class__.__name__
        == "ExternalFileEncryptionProperties"
    )
    assert external_encryption_properties.__class__.__module__ == "pyarrow._parquet"


def test_decryption_configuration_properties():
    """Test the standard DecryptionConfiguration properties to avoid regressions."""

    config = ppe.DecryptionConfiguration()
    config.cache_lifetime = datetime.timedelta(minutes=5.0)

    assert isinstance(config, ppe.DecryptionConfiguration)
    assert config.cache_lifetime == datetime.timedelta(minutes=5.0)


def test_external_decryption_configuration_properties():
    """Test the ExternalDecryptionConfiguration properties
      including external-specific fields."""

    external_decryption_config = get_external_decryption_config()
    assert isinstance(external_decryption_config, ppe.ExternalDecryptionConfiguration)
    assert external_decryption_config.cache_lifetime == datetime.timedelta(minutes=10.0)
    assert external_decryption_config.app_context == {
        "user_id": "Picard1701",
        "location": "Presidio"
    }
    assert external_decryption_config.connection_config == {
        "EXTERNAL_DBPA_V1": {
            "config_file": "path/to/config/file",
            "config_file_decryption_key": "some_key"
        }
    }


def test_external_decryption_connection_config_invalid_types():
    """Ensure connection_config rejects non-string keys or values."""

    # Outer key is not a string (int instead of cipher name string)
    with pytest.raises(AttributeError, match="'int' object has no attribute 'upper'"):
        config = ppe.ExternalDecryptionConfiguration()
        config.connection_config = {
            123: {  # invalid outer key
                "config_file": "should-fail"
            }
        }

    # Outer value is not a dict
    with pytest.raises(
        TypeError,
        match="Inner value for cipher AES_GCM_V1 must be a dict"
    ):
        config = ppe.ExternalDecryptionConfiguration()
        config.connection_config = {
            "AES_GCM_V1": ["not", "a", "dict"]  # invalid outer value (should be dict)
        }

    # Inner key is not a string
    with pytest.raises(TypeError, match="All inner config keys/values must be str"):
        config = ppe.ExternalDecryptionConfiguration()
        config.connection_config = {
            "AES_GCM_V1": {
                123: "should-fail"  # invalid inner key
            }
        }

    # Inner value is not a string
    with pytest.raises(TypeError, match="All inner config keys/values must be str"):
        config = ppe.ExternalDecryptionConfiguration()
        config.connection_config = {
            "AES_GCM_V1": {
                "config_file": ["not", "a", "string"]  # invalid inner value
            }
        }


def test_external_file_decryption_properties_valid():
    """Check class name and module because
    ExternalFileDecryptionProperties is not visible."""

    external_decryption_properties = get_external_decryption_properties()

    assert (
        external_decryption_properties.__class__.__name__
        == "ExternalFileDecryptionProperties"
    )
    assert external_decryption_properties.__class__.__module__ == "pyarrow._parquet"


"""
TODO(sbrenes): Re-enable test when ExternalDBPADecryptorAdapter
is integrated into Arrow.
def test_read_and_write_standard_encryption(tmp_path):
    Test a roundtrip encryption and decryption using standard encryption.

    data_table = get_data_table()
    parquet_path = tmp_path / "test.parquet"
    write_parquet(data_table, parquet_path, get_encryption_properties())

    read_data_table = read_parquet(parquet_path, get_decryption_properties())
    assert read_data_table.equals(data_table)
    assert read_data_table.num_rows == data_table.num_rows
    assert read_data_table.num_columns == data_table.num_columns
    assert read_data_table.schema.equals(data_table.schema)
    assert read_data_table.column_names == data_table.column_names


def test_read_and_write_external_encryption(tmp_path):
    Test a roundtrip encryption and decryption using external encryption.

    data_table = get_data_table()
    parquet_path = tmp_path / "test.parquet"
    write_parquet(data_table, parquet_path, get_external_encryption_properties())

    read_data_table = read_parquet(parquet_path, get_external_decryption_properties())
    assert read_data_table.equals(data_table)
    assert read_data_table.num_rows == data_table.num_rows
    assert read_data_table.num_columns == data_table.num_columns
    assert read_data_table.schema.equals(data_table.schema)
    assert read_data_table.column_names == data_table.column_names
"""
