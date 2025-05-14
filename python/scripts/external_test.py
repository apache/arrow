import base64
from datetime import timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as ppe


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

def get_kms_connection_config():
    return ppe.KmsConnectionConfig(
        custom_kms_conf={
            "footer_master_key_id": "012footer_secret",
            "master_key1": "column_secret001",
            "master_key2": "column_secret002"
        }
    )

#from pyarrow._parquet_encryption import (
#    ExternalEncryptionConfiguration
#)

# Step 1: Create Arrow arrays and schema
data = {
    "column1": pa.array([1, 2, 3], type=pa.int32()),
    "column2": pa.array(["a", "b", "c"], type=pa.string()),
    "column3": pa.array([True, False, True], type=pa.bool_())
}
table = pa.table(data)

# Step 2: Create the external encryption configuration
config = ppe.ExternalEncryptionConfiguration(
    footer_key=b"footer_master_key_id",
    column_keys={
        "master_key1": ["column1", "column2"],
        "master_key2": ["column3"]
    },
    encryption_algorithm="AES_GCM_V1",
    plaintext_footer=False,
    double_wrapping=True,
    cache_lifetime=timedelta(minutes=5),
    internal_key_material=True,
    data_key_length_bits=256,

    # External config fields
    host=b"https://kms.example.com",
    certificate_authority_location=b"/certs/ca.pem",
    client_certificate_location=b"/certs/client.pem",
    client_key_location=b"/certs/client.key",
    connection_pool_size=4,
    run_locally=False
)

# Step 3: Get encryption properties

crypto_factory = ppe.CryptoFactory(kms_client_factory)
file_encryption_props = crypto_factory.file_encryption_properties(get_kms_connection_config(), config)

print("-------------------------------------")
print(type(config))
print(config)

# Step 4: Write encrypted Parquet file
output_path = "sample17.parquet"
with pq.ParquetWriter(output_path, table.schema, encryption_properties=file_encryption_props) as writer:
    writer.write_table(table)

print(f"Encrypted file written to: {output_path}")


# Step 1: Recreate the same decryption config used for encryption
decryption_config = config

# Step 2: Create the decryption properties from the factory
file_decryption_props = crypto_factory.file_decryption_properties(decryption_config)

# Step 3: Read the encrypted Parquet file
input_path = "sample17.parquet"
table = pq.read_table(input_path, decryption_properties=file_decryption_props)

# Step 4: Print the contents of the table
for column_name in table.column_names:
    print(f"{column_name}: {table[column_name].to_pylist()}")
