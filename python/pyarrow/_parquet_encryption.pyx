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

# cython: profile=False
# distutils: language = c++

from datetime import timedelta

from cython.operator cimport dereference as deref

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport _Weakrefable
from pyarrow.lib import tobytes, frombytes

import json


cdef ParquetCipher cipher_from_name(name):
    name = name.upper()
    if name == 'AES_GCM_V1':
        return ParquetCipher_AES_GCM_V1
    elif name == 'AES_GCM_CTR_V1':
        return ParquetCipher_AES_GCM_CTR_V1
    elif name == 'EXTERNAL_DBPA_V1':
        return ParquetCipher_EXTERNAL_DBPA_V1
    else:
        raise ValueError(f'Invalid cipher name: {name}')


cdef cipher_to_name(ParquetCipher cipher):
    if ParquetCipher_AES_GCM_V1 == cipher:
        return 'AES_GCM_V1'
    elif ParquetCipher_AES_GCM_CTR_V1 == cipher:
        return 'AES_GCM_CTR_V1'
    elif ParquetCipher_EXTERNAL_DBPA_V1 == cipher:
        return 'EXTERNAL_DBPA_V1'
    else:
        raise ValueError('Invalid cipher value: {0}'.format(cipher))

cdef class EncryptionConfiguration(_Weakrefable):
    """Configuration of the encryption, such as which columns to encrypt"""
    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, footer_key, *, column_keys=None,
                 encryption_algorithm=None,
                 plaintext_footer=None, double_wrapping=None,
                 cache_lifetime=None, internal_key_material=None,
                 data_key_length_bits=None):
        self.configuration.reset(
            new CEncryptionConfiguration(tobytes(footer_key)))
        if column_keys is not None:
            self.column_keys = column_keys
        if encryption_algorithm is not None:
            self.encryption_algorithm = encryption_algorithm
        if plaintext_footer is not None:
            self.plaintext_footer = plaintext_footer
        if double_wrapping is not None:
            self.double_wrapping = double_wrapping
        if cache_lifetime is not None:
            self.cache_lifetime = cache_lifetime
        if internal_key_material is not None:
            self.internal_key_material = internal_key_material
        if data_key_length_bits is not None:
            self.data_key_length_bits = data_key_length_bits

    @property
    def footer_key(self):
        """ID of the master key for footer encryption/signing"""
        return frombytes(self.configuration.get().footer_key)

    @property
    def column_keys(self):
        """
        List of columns to encrypt, with master key IDs.
        """
        column_keys_str = frombytes(self.configuration.get().column_keys)
        # Convert from "masterKeyID:colName,colName;masterKeyID:colName..."
        # (see HIVE-21848) to dictionary of master key ID to column name lists
        column_keys_to_key_list_str = dict(subString.replace(" ", "").split(
            ":") for subString in column_keys_str.split(";"))
        column_keys_dict = {k: v.split(
            ",") for k, v in column_keys_to_key_list_str.items()}
        return column_keys_dict

    @column_keys.setter
    def column_keys(self, dict value):
        if value is not None:
            # convert a dictionary such as
            # '{"key1": ["col1 ", "col2"], "key2": ["col3 ", "col4"]}''
            # to the string defined by the spec
            # 'key1: col1 , col2; key2: col3 , col4'
            column_keys = "; ".join(
                ["{}: {}".format(k, ", ".join(v)) for k, v in value.items()])
            self.configuration.get().column_keys = tobytes(column_keys)

    @property
    def encryption_algorithm(self):
        """Parquet encryption algorithm.
        Can be "AES_GCM_V1" (default), or "AES_GCM_CTR_V1"."""
        return cipher_to_name(self.configuration.get().encryption_algorithm)

    @encryption_algorithm.setter
    def encryption_algorithm(self, value):
        cipher = cipher_from_name(value)
        self.configuration.get().encryption_algorithm = cipher

    @property
    def plaintext_footer(self):
        """Write files with plaintext footer."""
        return self.configuration.get().plaintext_footer

    @plaintext_footer.setter
    def plaintext_footer(self, value):
        self.configuration.get().plaintext_footer = value

    @property
    def double_wrapping(self):
        """Use double wrapping - where data encryption keys (DEKs) are
        encrypted with key encryption keys (KEKs), which in turn are
        encrypted with master keys.
        If set to false, use single wrapping - where DEKs are
        encrypted directly with master keys."""
        return self.configuration.get().double_wrapping

    @double_wrapping.setter
    def double_wrapping(self, value):
        self.configuration.get().double_wrapping = value

    @property
    def cache_lifetime(self):
        """Lifetime of cached entities (key encryption keys,
        local wrapping keys, KMS client objects)."""
        return timedelta(
            seconds=self.configuration.get().cache_lifetime_seconds)

    @cache_lifetime.setter
    def cache_lifetime(self, value):
        if not isinstance(value, timedelta):
            raise TypeError("cache_lifetime should be a timedelta")
        self.configuration.get().cache_lifetime_seconds = value.total_seconds()

    @property
    def internal_key_material(self):
        """Store key material inside Parquet file footers; this mode doesn’t
        produce additional files. If set to false, key material is stored in
        separate files in the same folder, which enables key rotation for
        immutable Parquet files."""
        return self.configuration.get().internal_key_material

    @internal_key_material.setter
    def internal_key_material(self, value):
        self.configuration.get().internal_key_material = value

    @property
    def data_key_length_bits(self):
        """Length of data encryption keys (DEKs), randomly generated by parquet key
        management tools. Can be 128, 192 or 256 bits."""
        return self.configuration.get().data_key_length_bits

    @data_key_length_bits.setter
    def data_key_length_bits(self, value):
        self.configuration.get().data_key_length_bits = value

    cdef inline shared_ptr[CEncryptionConfiguration] unwrap(self) nogil:
        return self.configuration

cdef class ExternalEncryptionConfiguration(EncryptionConfiguration):
    """ExternalEncryptionConfiguration inherits from EncryptionConfiguration."""
    __slots__ = ()
  
    def __init__(self, footer_key, *, column_keys=None,
                 encryption_algorithm=None,
                 plaintext_footer=None, double_wrapping=None,
                 cache_lifetime=None, internal_key_material=None,
                 data_key_length_bits=None, per_column_encryption=None,
                 app_context=None, connection_config=None):

        # Initialize pointer first so the get/set forwards work.
        self.external_configuration.reset(
            new CExternalEncryptionConfiguration(tobytes(footer_key)))

        super().__init__(footer_key,
            column_keys=column_keys,
            encryption_algorithm=encryption_algorithm,
            plaintext_footer=plaintext_footer,
            double_wrapping=double_wrapping,
            cache_lifetime=cache_lifetime,
            internal_key_material=internal_key_material,
            data_key_length_bits=data_key_length_bits)

        self.external_configuration.get().footer_key = \
            self.configuration.get().footer_key
        
        if app_context is not None:
            self.app_context = app_context
        if connection_config is not None:
            self.connection_config = connection_config
        if per_column_encryption is not None:
            self.per_column_encryption = per_column_encryption

    """ Forward all attributes get/set methods to the superclass """
    """ The superclass already converts to/from bytes and does additional processing needed """
    @property
    def column_keys(self):
        return EncryptionConfiguration.column_keys.__get__(self)

    @column_keys.setter
    def column_keys(self, dict value):
        EncryptionConfiguration.column_keys.__set__(self, value)
        self.external_configuration.get().column_keys = self.configuration.get().column_keys

    @property
    def encryption_algorithm(self):
        return EncryptionConfiguration.encryption_algorithm.__get__(self)

    @encryption_algorithm.setter
    def encryption_algorithm(self, value):
        EncryptionConfiguration.encryption_algorithm.__set__(self, value)
        self.external_configuration.get().encryption_algorithm = \
            self.configuration.get().encryption_algorithm

    @property
    def plaintext_footer(self):
        return EncryptionConfiguration.plaintext_footer.__get__(self)

    @plaintext_footer.setter
    def plaintext_footer(self, value):
        EncryptionConfiguration.plaintext_footer.__set__(self, value)
        self.external_configuration.get().plaintext_footer = value

    @property
    def double_wrapping(self):
        return EncryptionConfiguration.double_wrapping.__get__(self)

    @double_wrapping.setter
    def double_wrapping(self, value):
        EncryptionConfiguration.double_wrapping.__set__(self, value)
        self.external_configuration.get().double_wrapping = value

    @property
    def cache_lifetime(self):
        return EncryptionConfiguration.cache_lifetime.__get__(self)

    @cache_lifetime.setter
    def cache_lifetime(self, value):
        EncryptionConfiguration.cache_lifetime.__set__(self, value)
        self.external_configuration.get().cache_lifetime_seconds = value.total_seconds()

    @property
    def internal_key_material(self):
        return EncryptionConfiguration.internal_key_material.__get__(self)

    @internal_key_material.setter
    def internal_key_material(self, value):
        EncryptionConfiguration.internal_key_material.__set__(self, value)
        self.external_configuration.get().internal_key_material = value

    @property
    def data_key_length_bits(self):
        return EncryptionConfiguration.data_key_length_bits.__get__(self)

    @data_key_length_bits.setter
    def data_key_length_bits(self, value):
        EncryptionConfiguration.data_key_length_bits.__set__(self, value)
        self.external_configuration.get().data_key_length_bits = value

    @property
    def app_context(self):
        """Get the application context as a dictionary."""
        app_context_str = frombytes(self.external_configuration.get().app_context)
        if not app_context_str:
            return {}
        try:
            return json.loads(app_context_str)
        except Exception:
            raise ValueError(f"Invalid JSON stored in app_context: {app_context_str}")

    @app_context.setter
    def app_context(self, dict value):
        """Set the application context from a dictionary."""
        if value is None:
            raise ValueError("app_context must be JSON-serializable")

        try:
            serialized = json.dumps(value)               
            self.external_configuration.get().app_context = tobytes(serialized)
        except Exception:
            raise TypeError(f"Failed to serialize app_context: {repr(value)}")

    @property
    def connection_config(self):
        """Get the connection configuration as a Python dictionary."""

        cdef pair[ParquetCipher, unordered_map[c_string, c_string]] outer_pair
        cdef pair[c_string, c_string] inner_pair
        result = {}

        for outer_pair in self.external_configuration.get().connection_config:
            cipher_name = cipher_to_name(outer_pair.first)
            inner_map = {}
            for inner_pair in outer_pair.second:
                inner_map[frombytes(inner_pair.first)] = frombytes(inner_pair.second)
            result[cipher_name] = inner_map

        return result

    @connection_config.setter
    def connection_config(self, dict value):
        """Set the connection configuration from a Python dictionary."""
        if value is None:
            raise ValueError("Connection config value cannot be None")

        cdef unordered_map[ParquetCipher, unordered_map[c_string, c_string]] cpp_map
        cdef unordered_map[c_string, c_string] inner_cpp_map
        cdef ParquetCipher cipher_enum

        for cipher_name, inner_dict in value.items():
            cipher_enum = cipher_from_name(cipher_name)
            if not isinstance(inner_dict, dict):
                raise TypeError(f"Inner value for cipher {cipher_name} must be a dict")
            # Clear the map from the values of the previous iteration
            inner_cpp_map.clear()

            for k, v in inner_dict.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise TypeError("All inner config keys/values must be str")
                inner_cpp_map[tobytes(k)] = tobytes(v)
            cpp_map[cipher_enum] = inner_cpp_map
            
        self.external_configuration.get().connection_config = cpp_map

    @property
    def per_column_encryption(self):
        """Get the per_column_encryption as a Python dictionary."""

        py_dict = {}

        for pair in self.external_configuration.get().per_column_encryption:
            py_dict[frombytes(pair.first)] = {
                "encryption_algorithm": cipher_to_name(pair.second.parquet_cipher),
                "encryption_key": frombytes(pair.second.key_id)
            }

        return py_dict

    @per_column_encryption.setter
    def per_column_encryption(self, dict py_column_encryption):
        """Set the per_column_encryption from a Python dictionary."""
        if py_column_encryption is None:
            raise TypeError("per_column_encryption cannot be None")

        # Clear the existing C++ map first
        self.external_configuration.get().per_column_encryption.clear()

        cdef CColumnEncryptionAttributes cpp_attrs
        # Iterate over the Python dictionary
        for py_key, py_attrs in py_column_encryption.items():
            if not isinstance(py_key, str) or not isinstance(py_attrs, dict):
                raise TypeError("column_encryption keys must be strings and values must be dictionaries.")

            # Convert encryption_algorithm string to C++ ParquetCipher enum
            if "encryption_algorithm" not in py_attrs or not isinstance(py_attrs["encryption_algorithm"], str):
                raise ValueError("Each column must have 'encryption_algorithm' (string).")

            # Convert encryption_key string to C++ c_string
            if "encryption_key" not in py_attrs or not isinstance(py_attrs["encryption_key"], str):
                raise ValueError("Each column must have 'encryption_key' (string).")

            cpp_attrs.parquet_cipher = cipher_from_name(py_attrs["encryption_algorithm"])
            cpp_attrs.key_id = tobytes(py_attrs["encryption_key"])

            self.external_configuration.get().per_column_encryption[tobytes(py_key)] = cpp_attrs

    cdef inline shared_ptr[CExternalEncryptionConfiguration] unwrap_external(self) nogil:
        return self.external_configuration


cdef class DecryptionConfiguration(_Weakrefable):
    """Configuration of the decryption, such as cache timeout."""
    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, *, cache_lifetime=None):
        self.configuration.reset(new CDecryptionConfiguration())

    @property
    def cache_lifetime(self):
        """Lifetime of cached entities (key encryption keys,
        local wrapping keys, KMS client objects)."""
        return timedelta(
            seconds=self.configuration.get().cache_lifetime_seconds)

    @cache_lifetime.setter
    def cache_lifetime(self, value):
        self.configuration.get().cache_lifetime_seconds = value.total_seconds()

    cdef inline shared_ptr[CDecryptionConfiguration] unwrap(self) nogil:
        return self.configuration

cdef class ExternalDecryptionConfiguration(DecryptionConfiguration):
    """Configuration of the external decryption"""
    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, *, cache_lifetime=None, app_context=None, connection_config=None):
        # Initialize the pointer first so the get/set forwards work.
        # Super init will run the setters/getters below so we need the pointer to exist.
        self.external_configuration.reset(new CExternalDecryptionConfiguration())
        super().__init__(cache_lifetime=cache_lifetime)

        self.external_configuration.get().cache_lifetime_seconds = \
            self.configuration.get().cache_lifetime_seconds

        if app_context is not None:
            self.app_context = app_context
        if connection_config is not None:
            self.connection_config = connection_config
    
    """ Forward all attributes get/set methods to the superclass """
    """ The superclass already converts to/from bytes and does additional processing needed """
    @property
    def cache_lifetime(self):
        return DecryptionConfiguration.cache_lifetime.__get__(self)
    
    @cache_lifetime.setter
    def cache_lifetime(self, value):
        DecryptionConfiguration.cache_lifetime.__set__(self, value)
        self.external_configuration.get().cache_lifetime_seconds = value.total_seconds()

    @property
    def app_context(self):
        """Get the application context as a dictionary."""
        app_context_str = frombytes(self.external_configuration.get().app_context)
        if not app_context_str:
            return {}
        try:
            return json.loads(app_context_str)
        except Exception:
            raise ValueError(f"Invalid JSON stored in app_context: {app_context_str}")

    @app_context.setter
    def app_context(self, dict value):
        """Set the application context from a dictionary."""
        if value is None:
            raise ValueError("app_context must be JSON-serializable")

        try:
            serialized = json.dumps(value)               
            self.external_configuration.get().app_context = tobytes(serialized)
        except Exception:
            raise TypeError(f"Failed to serialize app_context: {repr(value)}")

    @property
    def connection_config(self):
        """Get the connection configuration as a Python dictionary."""

        cdef pair[ParquetCipher, unordered_map[c_string, c_string]] outer_pair
        cdef pair[c_string, c_string] inner_pair
        result = {}

        for outer_pair in self.external_configuration.get().connection_config:
            cipher_name = cipher_to_name(outer_pair.first)
            inner_map = {}
            for inner_pair in outer_pair.second:
                inner_map[frombytes(inner_pair.first)] = frombytes(inner_pair.second)
            result[cipher_name] = inner_map

        return result

    @connection_config.setter
    def connection_config(self, dict value):
        """Set the connection configuration from a Python dictionary."""
        if value is None:
            raise ValueError("Connection config value cannot be None")

        cdef unordered_map[ParquetCipher, unordered_map[c_string, c_string]] cpp_map
        cdef unordered_map[c_string, c_string] inner_cpp_map
        cdef ParquetCipher cipher_enum

        for cipher_name, inner_dict in value.items():
            cipher_enum = cipher_from_name(cipher_name)
            if not isinstance(inner_dict, dict):
                raise TypeError(f"Inner value for cipher {cipher_name} must be a dict")
            inner_cpp_map.clear()
            for k, v in inner_dict.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise TypeError("All inner config keys/values must be str")
                inner_cpp_map[tobytes(k)] = tobytes(v)
            cpp_map[cipher_enum] = inner_cpp_map

        self.external_configuration.get().connection_config = cpp_map

    cdef inline shared_ptr[CExternalDecryptionConfiguration] unwrap_external(self) nogil:
        return self.external_configuration

cdef class KmsConnectionConfig(_Weakrefable):
    """Configuration of the connection to the Key Management Service (KMS)"""
    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, *, kms_instance_id=None, kms_instance_url=None,
                 key_access_token=None, custom_kms_conf=None):
        self.configuration.reset(new CKmsConnectionConfig())
        if kms_instance_id is not None:
            self.kms_instance_id = kms_instance_id
        if kms_instance_url is not None:
            self.kms_instance_url = kms_instance_url
        if key_access_token is None:
            self.key_access_token = b'DEFAULT'
        else:
            self.key_access_token = key_access_token
        if custom_kms_conf is not None:
            self.custom_kms_conf = custom_kms_conf

    @property
    def kms_instance_id(self):
        """ID of the KMS instance that will be used for encryption
        (if multiple KMS instances are available)."""
        return frombytes(self.configuration.get().kms_instance_id)

    @kms_instance_id.setter
    def kms_instance_id(self, value):
        self.configuration.get().kms_instance_id = tobytes(value)

    @property
    def kms_instance_url(self):
        """URL of the KMS instance."""
        return frombytes(self.configuration.get().kms_instance_url)

    @kms_instance_url.setter
    def kms_instance_url(self, value):
        self.configuration.get().kms_instance_url = tobytes(value)

    @property
    def key_access_token(self):
        """Authorization token that will be passed to KMS."""
        return frombytes(self.configuration.get()
                         .refreshable_key_access_token.get().value())

    @key_access_token.setter
    def key_access_token(self, value):
        self.refresh_key_access_token(value)

    @property
    def custom_kms_conf(self):
        """A dictionary with KMS-type-specific configuration"""
        custom_kms_conf = {
            frombytes(k): frombytes(v)
            for k, v in self.configuration.get().custom_kms_conf
        }
        return custom_kms_conf

    @custom_kms_conf.setter
    def custom_kms_conf(self, dict value):
        if value is not None:
            for k, v in value.items():
                if isinstance(k, str) and isinstance(v, str):
                    self.configuration.get().custom_kms_conf[tobytes(k)] = \
                        tobytes(v)
                else:
                    raise TypeError("Expected custom_kms_conf to be " +
                                    "a dictionary of strings")

    def refresh_key_access_token(self, value):
        cdef:
            shared_ptr[CKeyAccessToken] c_key_access_token = \
                self.configuration.get().refreshable_key_access_token

        c_key_access_token.get().Refresh(tobytes(value))

    cdef inline shared_ptr[CKmsConnectionConfig] unwrap(self) nogil:
        return self.configuration

    @staticmethod
    cdef wrap(const CKmsConnectionConfig& config):
        result = KmsConnectionConfig()
        result.configuration = make_shared[CKmsConnectionConfig](move(config))
        return result


# Callback definitions for CPyKmsClientVtable
cdef void _cb_wrap_key(
        handler, const c_string& key_bytes,
        const c_string& master_key_identifier, c_string* out) except *:
    mkid_str = frombytes(master_key_identifier)
    wrapped_key = handler.wrap_key(key_bytes, mkid_str)
    out[0] = tobytes(wrapped_key)


cdef void _cb_unwrap_key(
        handler, const c_string& wrapped_key,
        const c_string& master_key_identifier, c_string* out) except *:
    mkid_str = frombytes(master_key_identifier)
    wk_str = frombytes(wrapped_key)
    key = handler.unwrap_key(wk_str, mkid_str)
    out[0] = tobytes(key)


cdef class KmsClient(_Weakrefable):
    """The abstract base class for KmsClient implementations."""
    cdef:
        shared_ptr[CKmsClient] client

    def __init__(self):
        self.init()

    cdef init(self):
        cdef:
            CPyKmsClientVtable vtable = CPyKmsClientVtable()

        vtable.wrap_key = _cb_wrap_key
        vtable.unwrap_key = _cb_unwrap_key

        self.client.reset(new CPyKmsClient(self, vtable))

    def wrap_key(self, key_bytes, master_key_identifier):
        """Wrap a key - encrypt it with the master key."""
        raise NotImplementedError()

    def unwrap_key(self, wrapped_key, master_key_identifier):
        """Unwrap a key - decrypt it with the master key."""
        raise NotImplementedError()

    cdef inline shared_ptr[CKmsClient] unwrap(self) nogil:
        return self.client


# Callback definition for CPyKmsClientFactoryVtable
cdef void _cb_create_kms_client(
        handler,
        const CKmsConnectionConfig& kms_connection_config,
        shared_ptr[CKmsClient]* out) except *:
    connection_config = KmsConnectionConfig.wrap(kms_connection_config)

    result = handler(connection_config)
    if not isinstance(result, KmsClient):
        raise TypeError(
            "callable must return KmsClient instances, but got {}".format(
                type(result)))

    out[0] = (<KmsClient> result).unwrap()


cdef class CryptoFactory(_Weakrefable):
    """ A factory that produces the low-level FileEncryptionProperties and
    FileDecryptionProperties objects, from the high-level parameters."""
    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, kms_client_factory):
        """Create CryptoFactory.

        Parameters
        ----------
        kms_client_factory : a callable that accepts KmsConnectionConfig
            and returns a KmsClient
        """
        self.factory.reset(new CPyCryptoFactory())

        if callable(kms_client_factory):
            self.init(kms_client_factory)
        else:
            raise TypeError("Parameter kms_client_factory must be a callable")

    cdef init(self, callable_client_factory):
        cdef:
            CPyKmsClientFactoryVtable vtable
            shared_ptr[CPyKmsClientFactory] kms_client_factory

        vtable.create_kms_client = _cb_create_kms_client
        kms_client_factory.reset(
            new CPyKmsClientFactory(callable_client_factory, vtable))
        # A KmsClientFactory object must be registered
        # via this method before calling any of
        # file_encryption_properties()/file_decryption_properties() methods.
        self.factory.get().RegisterKmsClientFactory(
            static_pointer_cast[CKmsClientFactory, CPyKmsClientFactory](
                kms_client_factory))

    def file_encryption_properties(self,
                                   KmsConnectionConfig kms_connection_config,
                                   EncryptionConfiguration encryption_config):
        """Create file encryption properties.

        Parameters
        ----------
        kms_connection_config : KmsConnectionConfig
            Configuration of connection to KMS

        encryption_config : EncryptionConfiguration
            Configuration of the encryption, such as which columns to encrypt

        Returns
        -------
        file_encryption_properties : FileEncryptionProperties
            File encryption properties.
        """
        cdef:
            CResult[shared_ptr[CFileEncryptionProperties]] \
                file_encryption_properties_result
        with nogil:
            file_encryption_properties_result = \
                self.factory.get().SafeGetFileEncryptionProperties(
                    deref(kms_connection_config.unwrap().get()),
                    deref(encryption_config.unwrap().get()))
        file_encryption_properties = GetResultValue(
            file_encryption_properties_result)
        return FileEncryptionProperties.wrap(file_encryption_properties)

    def external_file_encryption_properties(self,
                                            KmsConnectionConfig kms_connection_config,
                                            ExternalEncryptionConfiguration external_encryption_config):
        cdef:
            CResult[shared_ptr[CExternalFileEncryptionProperties]] \
                external_file_encryption_properties_result
        with nogil:
            external_file_encryption_properties_result = \
                self.factory.get().SafeGetExternalFileEncryptionProperties(
                    deref(kms_connection_config.unwrap().get()),
                    deref(external_encryption_config.unwrap_external().get()))
        external_file_encryption_properties = GetResultValue(
            external_file_encryption_properties_result)
        return ExternalFileEncryptionProperties.wrap_external(external_file_encryption_properties)
        
    def file_decryption_properties(
            self,
            KmsConnectionConfig kms_connection_config,
            DecryptionConfiguration decryption_config=None):
        """Create file decryption properties.

        Parameters
        ----------
        kms_connection_config : KmsConnectionConfig
            Configuration of connection to KMS

        decryption_config : DecryptionConfiguration, default None
            Configuration of the decryption, such as cache timeout.
            Can be None.

        Returns
        -------
        file_decryption_properties : FileDecryptionProperties
            File decryption properties.
        """
        cdef:
            CDecryptionConfiguration c_decryption_config
            CResult[shared_ptr[CFileDecryptionProperties]] \
                c_file_decryption_properties
        if decryption_config is None:
            c_decryption_config = CDecryptionConfiguration()
        else:
            c_decryption_config = deref(decryption_config.unwrap().get())
        with nogil:
            c_file_decryption_properties = \
                self.factory.get().SafeGetFileDecryptionProperties(
                    deref(kms_connection_config.unwrap().get()),
                    c_decryption_config)
        file_decryption_properties = GetResultValue(
            c_file_decryption_properties)
        return FileDecryptionProperties.wrap(file_decryption_properties)

    def external_file_decryption_properties(
            self,
            KmsConnectionConfig kms_connection_config,
            ExternalDecryptionConfiguration decryption_config):
        """Create file decryption properties.
        Parameters
        ----------
        kms_connection_config : KmsConnectionConfig
            Configuration of connection to KMS
        decryption_config : ExternalDecryptionConfiguration
            Configuration of the decryption, such as cache timeout and the information on how to connect the external decryption service.
        Returns
        -------
        file_decryption_properties : ExternalFileDecryptionProperties
            File decryption properties.
        """
        cdef:
            CExternalDecryptionConfiguration c_decryption_config
            CResult[shared_ptr[CExternalFileDecryptionProperties]] \
                c_file_decryption_properties
        if decryption_config is None:
            c_decryption_config = CExternalDecryptionConfiguration()
        else:
            c_decryption_config = deref(decryption_config.unwrap_external().get())
        with nogil:
            c_file_decryption_properties = \
                self.factory.get().SafeGetExternalFileDecryptionProperties(
                    deref(kms_connection_config.unwrap().get()),
                    c_decryption_config)
        file_decryption_properties = GetResultValue(
            c_file_decryption_properties)
        return ExternalFileDecryptionProperties.wrap_external(file_decryption_properties)

    def remove_cache_entries_for_token(self, access_token):
        self.factory.get().RemoveCacheEntriesForToken(tobytes(access_token))

    def remove_cache_entries_for_all_tokens(self):
        self.factory.get().RemoveCacheEntriesForAllTokens()

    cdef inline shared_ptr[CPyCryptoFactory] unwrap(self):
        return self.factory


cdef shared_ptr[CCryptoFactory] pyarrow_unwrap_cryptofactory(object crypto_factory) except *:
    if isinstance(crypto_factory, CryptoFactory):
        pycf = (<CryptoFactory> crypto_factory).unwrap()
        return static_pointer_cast[CCryptoFactory, CPyCryptoFactory](pycf)
    raise TypeError("Expected CryptoFactory, got %s" % type(crypto_factory))


cdef shared_ptr[CKmsConnectionConfig] pyarrow_unwrap_kmsconnectionconfig(object kmsconnectionconfig) except *:
    if isinstance(kmsconnectionconfig, KmsConnectionConfig):
        return (<KmsConnectionConfig> kmsconnectionconfig).unwrap()
    raise TypeError("Expected KmsConnectionConfig, got %s" % type(kmsconnectionconfig))


cdef shared_ptr[CEncryptionConfiguration] pyarrow_unwrap_encryptionconfig(object encryptionconfig) except *:
    if isinstance(encryptionconfig, EncryptionConfiguration):
        return (<EncryptionConfiguration> encryptionconfig).unwrap()
    raise TypeError("Expected EncryptionConfiguration, got %s" % type(encryptionconfig))

cdef shared_ptr[CExternalEncryptionConfiguration] pyarrow_unwrap_external_encryptionconfig(object encryptionconfig) except *:
    if isinstance(encryptionconfig, ExternalEncryptionConfiguration):
        return (<ExternalEncryptionConfiguration> encryptionconfig).unwrap_external()
    raise TypeError("Expected ExternalEncryptionConfiguration, got %s" % type(encryptionconfig))

cdef shared_ptr[CDecryptionConfiguration] pyarrow_unwrap_decryptionconfig(object decryptionconfig) except *:
    if isinstance(decryptionconfig, DecryptionConfiguration):
        return (<DecryptionConfiguration> decryptionconfig).unwrap()
    raise TypeError("Expected DecryptionConfiguration, got %s" % type(decryptionconfig))

cdef shared_ptr[CExternalDecryptionConfiguration] pyarrow_unwrap_external_decryptionconfig(object decryptionconfig) except *:
    if isinstance(decryptionconfig, ExternalDecryptionConfiguration):
        return (<ExternalDecryptionConfiguration> decryptionconfig).unwrap_external()
    raise TypeError("Expected ExternalDecryptionConfiguration, got %s" % type(decryptionconfig))