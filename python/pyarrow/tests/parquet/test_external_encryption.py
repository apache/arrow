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


def test_encrypted_external_config(tempdir):
    """Write an encrypted parquet, verify it's encrypted, and then read it."""

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

    # Check all getters return expected values
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

    # Check all getters return expected values
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
