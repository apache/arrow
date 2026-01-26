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

# ###################################################################################

"""
use_external_dbpa_encryption.py
Use this script as a template/guide to use the external DBPA encryption library
in Parquet Arrow.
"""

import base64
import datetime
import platform
import pyarrow
import pyarrow.parquet as pp
import pyarrow.parquet.encryption as ppe

# ###################################################################################

"""
A sample KMS client that uses a map of master keys to wrap and unwrap keys.
Replace this with a real KMS client or provider, the same way you would provide
this for regular Parquet Arrow encryption.
Make sure you include the column keys in the custom_kms_conf, even for those
columms that will be encrypted with the external DBPA agent.
"""


class FooKmsClient(ppe.KmsClient):

    def __init__(self, kms_connection_config):
        ppe.KmsClient.__init__(self)
        self.master_keys_map = kms_connection_config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_identifier):
        master_key_bytes = self.master_keys_map[master_key_identifier].encode(
            'utf-8')
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


def get_kms_connection_config():
    return ppe.KmsConnectionConfig(
        custom_kms_conf={
            "footer_key": "012footer_secret",
            "orderid_key": "column_secret001",
            "productid_key": "column_secret002",
            "price_key": "column_secret003",
            "customer_key": "column_secret004"
        }
    )


def kms_client_factory(kms_connection_config):
    return FooKmsClient(kms_connection_config)

# ###################################################################################


"""
Set up all encryption configuration parameters.
External DBPA encryption requires the use of the ExternalEncryptionConfiguration and
the ExternalFileEncryptionProperties classes. These allow you to specify per column
encryption algorithms and keys.
For this example, the following configuration will apply to each column:
- productId: encrypted withthe file-level encryption using AES_GCM_V1 and key
             productid_key
- orderId: encrypted with the AES_GCM_CTR_V1 encryption using key orderid_key
- price: encrypted with the external DBPA encryption using key price_key
- customer_name: encrypted with the external DBPA encryption using key customer_key
- vat: not encrypted
This is also where you send the application specific context to the external
encryptor, and  where you specify how to connect to the external DBPA encryptor:
whether via a library file, or via a remote service.
All other parameters are the same as for regular Parquet Arrow encryption.
"""


def get_external_encryption_config(use_remote_service):
    return ppe.ExternalEncryptionConfiguration(
        footer_key="footer_key",
        # File level encryption algorithm. This is the default algorithm that will
        # apply when no per column encryption algorithm is specified.
        encryption_algorithm="AES_GCM_V1",
        # These are the usual column keys that will be used for the file-level
        # encryption.
        column_keys={
            "productid_key": ["productId"]
        },
        cache_lifetime=datetime.timedelta(minutes=2.0),
        data_key_length_bits=128,
        plaintext_footer=True,
        # Specify each column's encryption algorithm and key. You can use any of the
        # encryption algorithms supported by Parquet Arrow. Keep in mind:
        # - A column may appear in either the column_keys or per_column_encryption,
        #   but not both.
        # - If a column appears in both, an exception will be thrown.
        # - If a column does not appear in either, it will not be encrypted.
        # - Any misspelling of a column name or algorithm name will result in an
        #   exception.
        per_column_encryption={
            "orderId": {
                "encryption_algorithm": "AES_GCM_CTR_V1",
                "encryption_key": "orderid_key"
            },
            "price": {
                "encryption_algorithm": "EXTERNAL_DBPA_V1",
                "encryption_key": "price_key"
            },
            "customer_name": {
                "encryption_algorithm": "EXTERNAL_DBPA_V1",
                "encryption_key": "customer_key"
            }
        },
        # Additional context for the external encryptor. Arrow will just forward
        # this value to the external encryptor, and does not read its contents.
        app_context=get_app_context(),
        # Configuration properties for the external encryptor.
        configuration_properties=get_dbpa_configuration_properties(
            use_remote_service)
    )


def get_external_file_encryption_properties(external_encryption_config):
    crypto_factory = ppe.CryptoFactory(kms_client_factory)
    return crypto_factory.external_file_encryption_properties(
        get_kms_connection_config(), external_encryption_config)

# ###################################################################################


"""
Set up all decryption configuration parameters.
External DBPA decryption requires the use of the ExternalDecryptionConfiguration and
the ExternalFileDecryptionProperties classes. These allow you to specify the
application specific context for the external decryptor, and how to instantiate the
external DBPA decryptor: whether via a local instance, or via a remote one.
All other parameters are the same as for regular Parquet Arrow decryption.
"""


def get_external_decryption_config(use_remote_service):
    return ppe.ExternalDecryptionConfiguration(
        cache_lifetime=datetime.timedelta(minutes=2.0),
        # Additional context for the external decryptor.
        app_context=get_app_context(),
        # Configuration properties for the external decryptor.
        configuration_properties=get_dbpa_configuration_properties(
            use_remote_service)
    )


def get_external_file_decryption_properties(external_decryption_config):
    crypto_factory = ppe.CryptoFactory(kms_client_factory)
    return crypto_factory.external_file_decryption_properties(
        get_kms_connection_config(), external_decryption_config)

# ###################################################################################


"""
Set up the application specific context for the external DBPA service. This is a
contract exclusively between the application and the external DBPA service. Arrow
makes no use of this.
"""


def get_app_context():
    return {
        "user_id": "Picard1701",
        "location": "Presidio"
    }

# ###################################################################################


"""
Set up the configuration properties for the external DBPA encryptor.
Each application can provide its own timeout values for the external DBPA encryptor
operations. If none are provided, default values are used on the encryptor side.
These timeout values are not network related, but rather a protection mechanism to
avoid the external encryptor or decryptor from taking too long to complete their
operations. The application must know the path to the external DBPA agent library
file. Important! Please ensure that LD_LIBRARY_PATH (or its equivalent) has been
modified to include the location of the shared library files for the external DBPA
encryptor and decryptor. When the external DBPA encryptor is running as a remote
service, the application must also provide the *absolute* path to the connection
config file, which must be a valid JSON that contains all the information needed to
connect to the external DBPA service. This includes the server URL and the
authentication credentials, which the application must procure on its own.
"""


def get_dbpa_configuration_properties(use_remote_service):
    configuration_properties = {
        "EXTERNAL_DBPA_V1": {
            "agent_init_timeout_ms": "15000",
            "agent_encrypt_timeout_ms": "35000",
            "agent_decrypt_timeout_ms": "35000"
        }
    }
    if use_remote_service:
        agent_library_path = (
            'libdbpsRemoteAgent.so'
            if platform.system() == 'Linux'
            else 'libdbpsRemoteAgent.dylib')
        configuration_properties["EXTERNAL_DBPA_V1"][
            "agent_library_path"
        ] = agent_library_path
        # Make sure this is the absolute path to the connection config file.
        remote_path = '/arrowdev/python/scripts/test_connection_config_file.json'
        configuration_properties["EXTERNAL_DBPA_V1"][
            "connection_config_file_path"
        ] = remote_path
    else:
        agent_library_path = (
            'libdbpsLocalAgent.so'
            if platform.system() == 'Linux'
            else 'libdbpsLocalAgent.dylib')
        configuration_properties["EXTERNAL_DBPA_V1"][
            "agent_library_path"
        ] = agent_library_path

    return configuration_properties

# ###################################################################################


"""
Write an encrypted Parquet file using the external DBPA encryption library.
The current implementation of the external DBPA library can support a per-value
encryption algorithm (which we call "best case encryption") only when the following
conditions are met:
- No compression algorithm is used.
- Dictionary encoding is disabled.
- Column encoding is set to PLAIN for all columns.
If any of these conditions are not met, the external DBPA library will perform
traditional per-page (as opposed to per-value) encryption.
"""


def write_encrypted_parquet_file(parquet_path, use_remote_service, scenario_id):
    print("\n------------------------------------------------------------")
    print(f"Writing encrypted parquet file to {parquet_path}")
    print(
        f"Using {'remote' if use_remote_service else 'local'} "
        "external DBPA encryptor service")
    print(f"Using scenario {scenario_id}")
    print("------------------------------------------------------------\n")

    sample_data = get_sample_data()
    encryption_config = get_external_encryption_config(use_remote_service)
    external_file_encryption_properties = get_external_file_encryption_properties(
        encryption_config)

    match scenario_id:
        case 1:
            # This is the simplest way to write an encrypted Parquet file. It will
            # use the default values for compression (SNAPPY) and
            # encoding (RLE_DICTIONARY).
            pp.write_table(sample_data, parquet_path,
                           encryption_properties=external_file_encryption_properties)
        case 2:
            # By specifying a combination of parameters that use a plain encoding
            # (not dictionary), and no compression, we can ensure that the current
            # external DBPA library will perform per-value encryption on the data.
            pp.write_table(sample_data, parquet_path,
                           encryption_properties=external_file_encryption_properties,
                           use_dictionary=False, compression="NONE")
        case 3:
            # Other parameters that can be specified involve the data page version
            # (which impacts how the data bytes are formatted), and specific
            # column encodings.
            pp.write_table(sample_data, parquet_path,
                           encryption_properties=external_file_encryption_properties,
                           data_page_version="2.0")

    print("\n------------------------------------------------------------")
    print(f"Encrypted parquet file written to {parquet_path}")
    print("------------------------------------------------------------\n")


def get_sample_data():
    # Creating a simple table for encryption. Use your real data, or load data
    # file as needed.
    return pyarrow.Table.from_pydict({
        "orderId": [1024, 1025, 1026],
        "productId": [152, 268, 6548],
        "price": [3.25, 6.48, 2.12],
        "vat": [0.0, 0.2, 0.05],
        "customer_name": ["Alice", "Bob", "Charlotte"]
    })

# ###################################################################################


"""
Read an encrypted parquet file and print the metadata and data table.
Use external file decryption properties to decrypt the parquet file. As with regular
Parquet Arrow decryption, there is no need to configure much, since the encryption
details are in the Parquet file metadata.
"""


def read_encrypted_parquet_file(parquet_path, use_remote_service):
    print("\n------------------------------------------------------------")
    print(f"Reading encrypted parquet file from {parquet_path}")
    print("------------------------------------------------------------\n")

    metadata = pp.read_metadata(parquet_path)
    print("\n------------------------------------------------------------")
    print(f"Decrypted parquet file metadata:\n {metadata}")
    print("------------------------------------------------------------\n")

    decryption_config = get_external_decryption_config(use_remote_service)
    external_file_decryption_properties = get_external_file_decryption_properties(
        decryption_config)
    parquet_file = pp.ParquetFile(
        parquet_path, decryption_properties=external_file_decryption_properties)
    data_table = parquet_file.read()
    print("\n------------------------------------------------------------")
    print(f"Decrypted data table:\n {data_table.to_pandas().head()}")
    print("------------------------------------------------------------\n")

# ###################################################################################


"""
Perform round trip encryption and decryption of example Parquet files.
We exercise the following cases for using the external DBPA encryptor services:
- scenario 1: default values for compression (SNAPPY) and encoding (DICTIONARY)
- scenario 2: plain encoding (not dictionary) and no compression
- scenario 3: data page version "2.0" and column encoding "BYTE_STREAM_SPLIT"
See the write_encrypted_parquet_file() function for more details on each scenario
and its implications.
"""


def round_trip_parquet_file_encryption():
    # for use_remote_service in [True, False]:
    for use_remote_service in [False]:
        service_path_prefix = 'remote' if use_remote_service else 'local'
        for scenario_id in [1, 2, 3]:
            parquet_path = (
                f"{service_path_prefix}_scenario_{scenario_id}_sample.parquet")
            write_encrypted_parquet_file(
                parquet_path, use_remote_service, scenario_id)
            read_encrypted_parquet_file(parquet_path, use_remote_service)

# ###################################################################################


"""
In order for the script to work, you must ensure that LD_LIBRARY_PATH (or its
equivalent) has been modified to include the location of the shared library files
for the external DBPA encryptor and decryptor.
"""
if __name__ == "__main__":
    print("\n------------------------------------------------------------")
    print("Using external DBPA encryption in Parquet Arrow")
    print("------------------------------------------------------------\n")
    round_trip_parquet_file_encryption()

# ###################################################################################
