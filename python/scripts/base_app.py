"""
base_app.py
@author sbrenes
"""

import base64
import datetime
import os
import pyarrow
import pyarrow.parquet as pp
import pyarrow.parquet.encryption as ppe
import platform

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
        raise ValueError(f"Bad master key used [{master_key_bytes}] - [{decrypted_key}]")


def kms_client_factory(kms_connection_config):
    return FooKmsClient(kms_connection_config)


def write_parquet(table, location, encryption_config=None):
    encryption_properties = None

    if encryption_config:
        crypto_factory = ppe.CryptoFactory(kms_client_factory)
        #encryption_properties = crypto_factory.external_file_encryption_properties(
        #    get_kms_connection_config(), encryption_config)
        encryption_properties = crypto_factory.file_encryption_properties(
            get_kms_connection_config(), encryption_config)

    # Uncomment the different possible cases for writing the parquet file. See 
    # https://github.com/protegrity/arrow/issues/204 for more details.

    # Case 1: Uncompressed data, using plain data encoding.
    print("\n!!! Writing uncompressed data, using plain data encoding. !!!\n")
    pp.write_table(table, location, use_dictionary=False,
        encryption_properties=encryption_properties, 
        compression="NONE")

    # Case 2: Compressed data, using RLE dictionary encoding.
    """
    print("\n!!! Writing compressed data, using RLE dictionary encoding. !!!\n")
    pp.write_table(table, location, use_dictionary=True,
        encryption_properties=encryption_properties, 
        compression="SNAPPY")
    """

    # Case 3: Uncompressed data, using RLE dictionary encoding.
    """
    print("\n!!! Writing uncompressed data, using RLE dictionary encoding. !!!\n")
    pp.write_table(table, location, use_dictionary=True,
        encryption_properties=encryption_properties, 
        compression="NONE")
    """

    # Case 4a: Compressed data, using plain data encoding and data page version 1.0.
    """
    print("\n!!! Writing compressed data, using plain data encoding and page version 1.0. !!!\n")
    pp.write_table(table, location, use_dictionary=False,
        encryption_properties=encryption_properties, 
        compression="SNAPPY")
    """

    # Case 4b: Compressed data, using plain data encoding and data page version 2.0.
    """
    print("\n!!! Writing compressed data, using plain data encoding and page version 2.0. !!!\n")
    pp.write_table(table, location, data_page_version="2.0", use_dictionary=False,
                   encryption_properties=encryption_properties, compression="SNAPPY")
    """


def encrypted_data_and_footer_sample(data_table):
    parquet_path = "sample.parquet"
    #encryption_config = get_external_encryption_config()
    encryption_config = get_encryption_config()
    write_parquet(data_table, parquet_path,
                  encryption_config=encryption_config)
    print(f"Written to [{parquet_path}]")


def create_and_encrypt_parquet():
    sample_data = {
        "orderId": [1001, 1002, 1003],
        "productId": [152, 268, 6548],
        "price": [3.25, 6.48, 2.12],
        "vat": [0.0, 0.2, 0.05],
        "customer_name": ["Alice", "Bob", "Charlotte"]
    }
    data_table = pyarrow.Table.from_pydict(sample_data)

    print("\nWriting parquet.")

    encrypted_data_and_footer_sample(data_table)


def read_and_print_parquet():
    print("\n-----------------------------------------------\nNow reading parquet file")
    parquet_path = "sample.parquet"

    metadata = pp.read_metadata(parquet_path)
    print("\nMetadata:")
    print(metadata)
    print("\n")

    #decryption_config = get_external_decryption_config()
    decryption_config = get_decryption_config()
    read_data_table = read_parquet(parquet_path,
                                   decryption_config=decryption_config)
    data_frame = read_data_table.to_pandas()
    print("\Decrypted data:")
    print(data_frame.head())
    print("\n")


def read_parquet(location, decryption_config=None, read_metadata=False):
    decryption_properties = None

    if decryption_config:
        crypto_factory = ppe.CryptoFactory(kms_client_factory)
        #decryption_properties = crypto_factory.external_file_decryption_properties(
        #    get_kms_connection_config(), decryption_config)
        decryption_properties = crypto_factory.file_decryption_properties(
            get_kms_connection_config(), decryption_config)

    if read_metadata:
        metadata = pp.read_metadata(location, decryption_properties=decryption_properties)
        return metadata

    data_table = pp.ParquetFile(location, decryption_properties=decryption_properties).read()
    return data_table


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

def get_external_encryption_config(plaintext_footer=True):
    return ppe.ExternalEncryptionConfiguration(
        footer_key = "footer_key",
        column_keys = {
            "productid_key": ["productId"]
        },
        encryption_algorithm = "AES_GCM_V1",
        cache_lifetime=datetime.timedelta(minutes=2.0),
        data_key_length_bits = 128,
        plaintext_footer=plaintext_footer,
        per_column_encryption = {
            "orderId": {
                "encryption_algorithm": "EXTERNAL_DBPA_V1",
                "encryption_key": "orderid_key"
            },
            "price": {
                "encryption_algorithm": "EXTERNAL_DBPA_V1", #"AES_GCM_CTR_V1",
                "encryption_key": "price_key"
            },
            "customer_name": {
                "encryption_algorithm": "EXTERNAL_DBPA_V1",
                "encryption_key": "customer_key"
            }
        },
        app_context = {
            "user_id": "Picard1701",
            "location": "Presidio"
        },
        connection_config = get_dbpa_connection_config()
    )

def get_encryption_config(plaintext_footer=True):
    return ppe.EncryptionConfiguration(
        footer_key = "footer_key",
        column_keys = {
            "orderid_key": ["orderId"],
            "productid_key": ["productId"]
        },
        encryption_algorithm = "AES_GCM_V1",
        cache_lifetime=datetime.timedelta(minutes=2.0),
        data_key_length_bits = 128,
        plaintext_footer=plaintext_footer
    )

def get_decryption_config():
    return ppe.DecryptionConfiguration(cache_lifetime=datetime.timedelta(minutes=2.0))

def get_external_decryption_config():
    return ppe.ExternalDecryptionConfiguration(
        cache_lifetime=datetime.timedelta(minutes=2.0),
        app_context = {
            "user_id": "Picard1701",
            "location": "Presidio"
        },
        connection_config = get_dbpa_connection_config()
    )

def get_config_file():
    config_file_name = os.environ.get('DBPA_CONFIG_FILE_NAME', 'test_connection_config_file.json')

    #verify if the file exists (assuming full path)
    if os.path.exists(config_file_name):
        return config_file_name

    #did not find the file. check if it exists in the same directory as the script
    script_directory = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_directory, config_file_name)
    if os.path.exists(config_path):
        return config_path

    #did not find the file. return None and let the caller handle it.
    #throw an error

    raise FileNotFoundError(f"Connection config [{config_file_name}] file not found")


def get_dbpa_connection_config():
    #we read the name of the external DBPA agent library from the environment variable DBPA_LIBRARY_PATH.
    #if not available, we use the default to 'libDBPATestAgent.so'.
    #this library performs key-independent, XOR encryption/decryption, and is built as part of the Parquet Arrow tests.
    #It is located in cpp/src/parquet/encryption/external/dbpa_test_agent.cc
    agent_library_path = os.environ.get(
        'DBPA_LIBRARY_PATH', 
        'libDBPATestAgent.so' if platform.system() == 'Linux' else 'libDBPATestAgent.dylib')

    connection_config = {
        "EXTERNAL_DBPA_V1": {
            "agent_library_path": agent_library_path,
            "agent_init_timeout_ms": "15000",
            "agent_encrypt_timeout_ms": "35000",
            "agent_decrypt_timeout_ms": "35000"
        }
    }

    #TODO: need a better way to perform this check
    config_file_required = "remote" in agent_library_path.lower()

    if (config_file_required):
        config_path = get_config_file()
        connection_config["EXTERNAL_DBPA_V1"]["connection_config_file_path"] = config_path

    return connection_config


if __name__ == "__main__":
    create_and_encrypt_parquet()
    read_and_print_parquet()
    print("\nPlayground finished!\n")