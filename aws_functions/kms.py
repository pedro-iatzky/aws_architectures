"""
This module is meant to provide AWS KMS infrastructure functions
"""
import boto3
from base64 import b64decode, b64encode

_CLIENT = boto3.client('kms')


DEFAULT_KEY = 'arn:aws:kms:us-west-2:some_key'


def encrypt_string(string_to_encrypt, cmk_id=DEFAULT_KEY):
    """
    Return the string encrypted and in base 64 format
    :param string_to_encrypt: <str>
    :param cmk_id: <str>. The aws CMK key_id or ARN
    :return: <str>
    """
    response = _CLIENT.encrypt(KeyId=cmk_id, Plaintext=string_to_encrypt)
    ciphertext_b64 = b64encode(response['CiphertextBlob']).decode('utf-8')
    return ciphertext_b64


def encrypt_dictionary(dictionary, cmk_id=DEFAULT_KEY):
    """
    Return an encrypted dictionary, with the same keys but encrypted values
    :param dictionary: <dict>. The original decrypted dictionary
    :param cmk_id: <str>. The aws CMK key_id or ARN
    :return: <dict>
    """
    encrypted_dict = {}
    for key, value in dictionary.items():
        encrypted_dict[key] = encrypt_string(value, cmk_id=cmk_id)
    return encrypted_dict


def decrypt_string(encrypted_b64str):
    """
    Return a decrypted string for an encrypted one with an AWS CMK
    :param encrypted_b64str: <str>. The encrypted string, in Base64 format
    :return: <str>
    """
    response = _CLIENT.decrypt(CiphertextBlob=b64decode(encrypted_b64str))
    return response['Plaintext'].decode('utf-8')


def decrypt_dictionary(encrypted_dict):
    """
    Return another dictionary with the same keys, but the values decrypted
    :param encrypted_dict: <dict>. The dictionary whose values are encrypted
    :return: <dict>
    """
    decrypted_dict = {}
    for key, value in encrypted_dict.items():
        decrypted_dict[key] = decrypt_string(value)
    return decrypted_dict
