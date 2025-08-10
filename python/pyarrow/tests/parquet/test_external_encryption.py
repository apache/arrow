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
import pytest
from datetime import  timedelta
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
from pyarrow._parquet_encryption import CryptoFactory, KmsConnectionConfig, ExternalEncryptionConfiguration


def test_encryption_configuration_properties(tempdir):
    """Test the standard EncryptionConfiguration properties to avoid regressions."""

    config = pe.EncryptionConfiguration(
        footer_key="footer-key-name",
        column_keys={
            "col-key-id": ["a", "b"],
        },
        encryption_algorithm="AES_GCM_V1",
        plaintext_footer=True,
        double_wrapping=True,
        cache_lifetime=timedelta(minutes=5.0),
        internal_key_material=True,
        data_key_length_bits=256
    )

    assert isinstance(config, pe.EncryptionConfiguration)

    assert config.footer_key == "footer-key-name"
    assert config.column_keys == {
        "col-key-id": ["a", "b"]
    }
    assert config.encryption_algorithm == "AES_GCM_V1"
    assert config.plaintext_footer is True
    assert config.double_wrapping is True
    assert config.cache_lifetime == timedelta(minutes=5.0)
    assert config.internal_key_material is True
    assert config.data_key_length_bits == 256

def test_external_encryption_configuration_properties(tempdir):
    """Test the ExternalEncryptionConfiguration properties including external-specific fields."""

    config_external = pe.ExternalEncryptionConfiguration(
        footer_key="footer-key-name",
        column_keys={
            "col-key-id": ["a", "b"],
        },
        encryption_algorithm="AES_GCM_V1",
        plaintext_footer=True,
        double_wrapping=True,
        cache_lifetime=timedelta(minutes=5.0),
        internal_key_material=True,
        data_key_length_bits=256,
        per_column_encryption={
            "a": {
                "encryption_algorithm": "AES_GCM_V1",
                "encryption_key": "key_1"
            },
            "b": {
                "encryption_algorithm": "AES_GCM_CTR_V1",
                "encryption_key": "key_n"
            }
        },
        app_context={
            "user_id": "Picard1701",
            "location": "Presidio"
        },
        connection_config={
            "config_file": "path/to/config/file",
            "config_file_decryption_key": "some_key"
        }
    )

    assert isinstance(config_external, pe.ExternalEncryptionConfiguration)

    assert config_external.footer_key == "footer-key-name"
    assert config_external.column_keys == {
        "col-key-id": ["a", "b"]
    }
    assert config_external.encryption_algorithm == "AES_GCM_V1"
    assert config_external.plaintext_footer is True
    assert config_external.double_wrapping is True
    assert config_external.cache_lifetime == timedelta(minutes=5.0)
    assert config_external.internal_key_material is True
    assert config_external.data_key_length_bits == 256

    assert config_external.app_context == {
        "user_id": "Picard1701",
        "location": "Presidio"
    }

    assert config_external.connection_config == {
        "config_file": "path/to/config/file",
        "config_file_decryption_key": "some_key"
    }

    assert config_external.per_column_encryption == {
        "a": {
            "encryption_algorithm": "AES_GCM_V1",
            "encryption_key": "key_1"
        },
        "b": {
            "encryption_algorithm": "AES_GCM_CTR_V1",
            "encryption_key": "key_n"
        }
    }



def test_external_encryption_app_context_invalid_json():
    """Ensure app_context raises TypeError for non-JSON-serializable input."""
    with pytest.raises(TypeError, match="Failed to serialize app_context: {'invalid': {1, 2, 3}}"):
        pe.ExternalEncryptionConfiguration(
            footer_key="key",
            app_context={"invalid": set([1, 2, 3])}  # sets are not JSON-serializable
        )

def test_external_encryption_per_column_encryption_invalid_algorithm():
    """Ensure invalid encryption_algorithm raises a ValueError or is rejected."""

    with pytest.raises(ValueError, match="Invalid cipher name: 'INVALID'"):
        pe.ExternalEncryptionConfiguration(
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

    pe.ExternalEncryptionConfiguration(
        footer_key="key",
        per_column_encryption={
            "a": {
                "encryption_algorithm": "EXTERNAL_DBPA_V1",
                "encryption_key": "some_key"
            }
        }
    )

def test_external_encryption_connection_config_invalid_types():
    """Ensure connection_config rejects non-string keys or values."""
    with pytest.raises(TypeError, match="Connection config key must be str, got int"):
        config=pe.ExternalEncryptionConfiguration(
            footer_key="key"
        )
        config.connection_config={
                "config_file": "path/to/file",
                123: "should-fail"  # Invalid: key is not a string
            }

    with pytest.raises(TypeError, match="Connection config value must be str, got list"):
        config = pe.ExternalEncryptionConfiguration(
            footer_key="key"
        )
        config.connection_config={
                "config_file": ["not", "a", "string"]  # Invalid: value is not a string
            }

def test_external_encryption_rejects_none_values():
    config = pe.ExternalEncryptionConfiguration(footer_key="key")

    # per_column_encryption: expect ValueError
    with pytest.raises(TypeError, match="per_column_encryption cannot be None"):
        config.per_column_encryption = None

    # app_context: expect ValueError due to None not being JSON-serializable
    with pytest.raises(ValueError, match="app_context must be JSON-serializable"):
        config.app_context = None

    # connection_config: expect ValueError due to None not being iterable
    with pytest.raises(ValueError, match="Connection config value cannot be None"):
        config.connection_config = None


class DummyKmsClient(pe.KmsClient):
    def __init__(self):
        super().__init__()

    def wrap_key(self, key_bytes, master_key_identifier):
        # dummy wrap just returns key_bytes
        return key_bytes

    def unwrap_key(self, wrapped_key, master_key_identifier):
        # dummy unwrap just returns wrapped_key
        return wrapped_key

@pytest.fixture
def kms_config():
    return pe.KmsConnectionConfig(
        custom_kms_conf={
            "footer_key": "012footer_secret",
            "orderid_key": "column_secret001",
            "productid_key": "column_secret002"
        }
    )

@pytest.fixture
def external_encryption_config():
    return pe.ExternalEncryptionConfiguration(
        footer_key=b"0123456789abcdef",  # exactly 16 bytes
        column_keys={
            "col-key-id": ["a", "b"],
        },
        encryption_algorithm="AES_GCM_V1",
        plaintext_footer=True,
        double_wrapping=True,
        cache_lifetime=timedelta(minutes=5.0),
        internal_key_material=True,
        data_key_length_bits=256,
        per_column_encryption={
            "a": {
                "encryption_algorithm": "AES_GCM_V1",
                "encryption_key": "key_1"
            },
            "b": {
                "encryption_algorithm": "AES_GCM_CTR_V1",
                "encryption_key": "key_n"
            }
        },
        app_context={
            "user_id": "Picard1701",
            "location": "Presidio"
        },
        connection_config={
            "config_file": "path/to/config/file",
            "config_file_decryption_key": "some_key"
        }
    )

def kms_client_factory(kms_connection_config):
    # For test: just return a DummyKmsClient instance, ignoring the config
    return DummyKmsClient()

def test_external_file_encryption_properties_valid(kms_config, external_encryption_config):
    factory = pe.CryptoFactory(kms_client_factory)
    result = factory.external_file_encryption_properties(kms_config, external_encryption_config)

    # Instead of isinstance, check class name and module dynamically because ExternalEncryptionConfiguration is not visbile
    assert result.__class__.__name__ == "ExternalFileEncryptionProperties"
    assert result.__class__.__module__ == "pyarrow._parquet"

