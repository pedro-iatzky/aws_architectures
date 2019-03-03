"""
This module is meant to provide AWS S3 infrastructure functions
"""
import os
from urllib.parse import urlparse, ParseResult

import boto3

_CLIENT = boto3.client('s3')


def get_bucket_and_key(s3_uri):
    """
    Returns the bucket and the key for a given s3 resource's uri.
    :param s3_uri: <urllib.parse.ParseResult> or <str>.
        E.g. s3://some_bucket/some_key_prefix
    :return:
    """
    s3_uri = s3_uri if isinstance(s3_uri, ParseResult) else urlparse(s3_uri)
    bucket = s3_uri.netloc
    key = s3_uri.path[1:]

    return bucket, key


def download_resource(s3_uri, dest_dir):
    """
    Download a s3 file into a given destiny directory.
    :param s3_uri: <urllib.parse.ParseResult> or <str>.
        E.g. s3://some_bucket/some_key_prefix
    :param dest_dir: <str>. The destination directory where the file
        is going to be downloaded.
    :return:
    """
    bucket, key = get_bucket_and_key(s3_uri)
    local_key = os.path.join(*key.split('/'))
    dest_path = os.path.join(dest_dir, bucket, local_key)
    if os.path.exists(dest_path):
        raise FileExistsError('The file is already downloaded.')
    if not os.path.exists(os.path.dirname(dest_path)):
        os.makedirs(os.path.dirname(dest_path))
    _CLIENT.download_file(bucket, key, dest_path)


def get_blob(s3_uri):
    """
    It returns a s3 object data stream.
    :param s3_uri: <urllib.parse.ParseResult> or <str>.
        E.g. s3://some_bucket/some_key_prefix
    :return:
    """
    bucket, key = get_bucket_and_key(s3_uri)
    response = _CLIENT.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()


def __list_objects_with_token(bucket, key, _continuation_token):
    """
    This function uses recursion in order to return all the objects whenever
     there is a continuation token.
    :param bucket: <str>
    :param key: <str>
    :param _continuation_token: <str>
    :return:
    """
    __sub_response = _CLIENT.list_objects_v2(
        Bucket=bucket, Prefix=key,  ContinuationToken=_continuation_token
    )
    __sub_list = __sub_response['Contents']
    if __sub_response['IsTruncated']:
        extended_list = __list_objects_with_token(
            bucket, key, _continuation_token=__sub_response['NextContinuationToken']
        )
        final_list = __sub_list + extended_list
    else:
        final_list = __sub_list
    return final_list


def list_objects(uri_prefix):
    """
    It lists all objects present in s3 that matches a defined uri prefix
    :param uri_prefix: <urllib.parse.ParseResult> or <str>.
        E.g. s3://some_bucket/some_key_prefix
    :return: <dict>
    """
    bucket, key = get_bucket_and_key(uri_prefix)
    response = _CLIENT.list_objects_v2(Bucket=bucket, Prefix=key)
    try:
        _list = response['Contents']
    except KeyError:
        raise ValueError('The object is not present in S3. Check out that '
                         'the uri you passed as argument being valid')
    if response['IsTruncated']:
        # Because list_objects function only returns up to 1000 objects, we have
        # to call recursively the function.
        return _list + __list_objects_with_token(
            bucket, key, _continuation_token=response['NextContinuationToken']
        )
    else:
        return _list


def list_keys(uri):
    """
    It lists all object's keys in s3 that matches a defined uri prefix
    :param uri:  <str>. E.g. s3://some_bucket/some_key_prefix
    :return:
    """
    return [objs_meta['Key'] for objs_meta in list_objects(uri)]


def make_public_read(s3_uri):
    """
    Makes a s3 resource public readable.
    :param s3_uri: <urllib.parse.ParseResult> or <str>.
        E.g. s3://some_bucket/some_key_prefix
    :return:
    """
    bucket, key = get_bucket_and_key(s3_uri)
    return _CLIENT.put_object_acl(ACL='public-read', Bucket=bucket, Key=key)


def get_object_acl(s3_uri):
    """
    Get the access control list for a given s3 resource.
    :param s3_uri: <urllib.parse.ParseResult> or <str>.
        E.g. s3://some_bucket/some_key_prefix
    :return:
    """
    bucket, key = get_bucket_and_key(s3_uri)
    return _CLIENT.get_object_acl(Bucket=bucket, Key=key)
