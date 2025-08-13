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

@pytest.fixture
def external_decryption_config():
    config = pe.ExternalDecryptionConfiguration(
        cache_lifetime=timedelta(minutes=5.0),
        app_context={
            "user_id": "Picard1701",
            "location": "Presidio"
        },
        connection_config={
            "AES_GCM_V1": {
                "config_file": "path/to/config/file",
                "config_file_decryption_key": "some_key"
            }
        }
    )
    config.cache_lifetime=timedelta(minutes=5.0)
    return config

def test_encryption_configuration_properties():
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

def test_external_encryption_configuration_properties(external_encryption_config):
    """Test the ExternalEncryptionConfiguration properties including external-specific fields."""

    assert isinstance(external_encryption_config, pe.ExternalEncryptionConfiguration)

    assert external_encryption_config.footer_key == "0123456789abcdef"
    assert external_encryption_config.column_keys == {
        "col-key-id": ["a", "b"]
    }
    assert external_encryption_config.encryption_algorithm == "AES_GCM_V1"
    assert external_encryption_config.plaintext_footer is True
    assert external_encryption_config.double_wrapping is True
    assert external_encryption_config.cache_lifetime == timedelta(minutes=5.0)
    assert external_encryption_config.internal_key_material is True
    assert external_encryption_config.data_key_length_bits == 256

    assert external_encryption_config.app_context == {
        "user_id": "Picard1701",
        "location": "Presidio"
    }

    assert external_encryption_config.connection_config == {
        "config_file": "path/to/config/file",
        "config_file_decryption_key": "some_key"
    }

    assert external_encryption_config.per_column_encryption == {
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

    with pytest.raises(ValueError, match="Invalid cipher name: INVALID"):
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

def test_external_file_encryption_properties_valid(external_encryption_config):
    class DummyKmsClient(pe.KmsClient):
        def __init__(self, kms_connection_config):
            super().__init__()

        def wrap_key(self, key_bytes, master_key_identifier):
            # dummy wrap just returns key_bytes
            return key_bytes

        def unwrap_key(self, wrapped_key, master_key_identifier):
            # dummy unwrap just returns wrapped_key
            return wrapped_key
    
    kms_config = pe.KmsConnectionConfig(
        custom_kms_conf={
            "footer_key": "012footer_secret",
            "orderid_key": "column_secret001",
            "productid_key": "column_secret002"
        }
    )

    factory = pe.CryptoFactory(DummyKmsClient)
    result = factory.external_file_encryption_properties(kms_config, external_encryption_config)

    # Instead of isinstance, check class name and module dynamically because ExternalFileEncryptionProperties is not visbile
    assert result.__class__.__name__ == "ExternalFileEncryptionProperties"
    assert result.__class__.__module__ == "pyarrow._parquet"

def test_decryption_configuration_properties():
    """Test the standard DecryptionConfiguration properties to avoid regressions."""

    config = pe.DecryptionConfiguration()
    config.cache_lifetime=timedelta(minutes=5.0)

    assert isinstance(config, pe.DecryptionConfiguration)
    assert config.cache_lifetime == timedelta(minutes=5.0)

def test_external_decryption_configuration_properties(external_decryption_config):
    """Test the ExternalDecryptionConfiguration properties including external-specific fields."""

    config = external_decryption_config

    assert isinstance(config, pe.ExternalDecryptionConfiguration)
    assert config.cache_lifetime == timedelta(minutes=5.0)
    assert config.app_context == {
        "user_id": "Picard1701",
        "location": "Presidio"
    }
    assert config.connection_config == {
        "AES_GCM_V1": {
            "config_file": "path/to/config/file",
            "config_file_decryption_key": "some_key"
        }
    }
    
def test_external_decryption_connection_config_invalid_types():
    """Ensure connection_config rejects non-string keys or values."""

    # Outer key is not a string (int instead of cipher name string)
    with pytest.raises(AttributeError, match="'int' object has no attribute 'upper'"):
        config = pe.ExternalDecryptionConfiguration()
        config.connection_config = {
            123: {  # invalid outer key
                "config_file": "should-fail"
            }
        }
    
    # Outer value is not a dict
    with pytest.raises(TypeError, match="Inner value for cipher AES_GCM_V1 must be a dict"):
        config = pe.ExternalDecryptionConfiguration()
        config.connection_config = {
            "AES_GCM_V1": ["not", "a", "dict"]  # invalid outer value (should be dict)
        }

    # Inner key is not a string
    with pytest.raises(TypeError, match="All inner config keys/values must be str"):
        config = pe.ExternalDecryptionConfiguration()
        config.connection_config = {
            "AES_GCM_V1": {
                123: "should-fail"  # invalid inner key
            }
        }

    # Inner value is not a string
    with pytest.raises(TypeError, match="All inner config keys/values must be str"):
        config = pe.ExternalDecryptionConfiguration()
        config.connection_config = {
            "AES_GCM_V1": {
                "config_file": ["not", "a", "string"]  # invalid inner value
            }
        }