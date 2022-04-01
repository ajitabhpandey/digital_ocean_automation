import logging

import boto3.session
import botocore.exceptions

logger = logging.getLogger(__name__)

def new_s3_client(region, endpoint_url, access_key, secret_key):
    """Initialize an S3 client with a private session so that multithreading
    doesn't cause issues with the client's internal state

    Parameters:
        region: str, region where operaion is to be performed
        endpoint_url: str, URL of the S3 endpoint
        access_key: str, API Access Key to access resource
        secret_key: str, API Secret Key to access resource

    Returns:
        An s3 client object
    """
    # initialize an S3 client with a private session so that multithreading
    # doesn't cause issues with the client's internal state
    try:
        session = boto3.session.Session()
        return session.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    except Exception as e:
        logger.error(f"Error while initiating session - {e}")

def is_file_present(client, bucket, prefix, key):
    """Return the file's size if it exist, else None

    Parameters:
        client: str, the boto3 client object
        bucket: str, target bucket location
        prefix: str, folder name under which file is to be checked
        key: str, filename which is to be checked for presence at folder

    Returns:
        True indicates an exception
        False indicates target object does not exists
        Size of the file if it exists
    """
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if response.get("Contents") is None:
        logger.error(
            f"The target folder {prefix} may not exists in the bucket {bucket}"
        )
        return False
    else:
        try:
            for obj in response.get("Contents"):
                # The obj['Key'] will have the file name prefixed with the prefix(folder)
                # since S3 and Spaces do not have concept of folders. We will remove the
                # prefix before comparing the file name
                if obj["Key"].replace(prefix, "") == key:
                    return obj["Size"]
        except Exception as e:
            logger.error(f"Unknown Exception - {e}")
            # A return value of True will indicate object exists, even though it may not.
            # This will prevent upload in case of an exception.
            return True


def upload_to_object_store(
    client,
    bucket,
    full_path_to_filename,
    object_name=None,
    content_type="binary/octet-stream",
):
    """Uploads the specified file to the object store

    Parameters:
        client: str, the boto3 client object
        bucket: str, target bucket location
        full_path_to_filename: str, full path of the file to be uploaded
        object_name: str, the new name of the file at the target location. full_path_to_filename is used if not specified
        content_type: str, the content type of the file. Default value of binary/octet-stream is used if not specified

    Returns:
        False if the upload fails
        True if the upload succeeds
    """
    try:
        # Upload the file to Spaces
        client.upload_file(
            full_path_to_filename,
            bucket,
            object_name,
            ExtraArgs={"ACL": "private", "ContentType": content_type},
        )
    except Exception as e:
        logger.error(f"Exception while uploading file - {e}")
        return False
    else:
        return True


def list_all_objects_older_than_last_modified(
    client, bucket, prefix, last_modified_timestamp
):
    """Lists all files in the object store older than the specified last modified timestamp

    Parameters:
        client: str, the boto3 client object
        bucket: str, target bucket location
        prefix: str, folder name under which file is to be checked
        last_modified_timestamp: datetime, last modified timestamp in utc

    Returns:
        Empty list if the objects are not found
        List of objects found
    """
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    try:
        filtered_file_names = []
        matching_object_counter = 0
        for page in pages:
            for obj in page["Contents"]:
                if str(obj["LastModified"]) < str(last_modified_timestamp):
                    logger.debug(f'{obj["Key"]} - {str(obj["LastModified"])}')
                    matching_object_counter = matching_object_counter + 1
                    # full_s3_file = bucket + "/" + prefix + obj["Key"]
                    full_s3_file = obj["Key"]
                    filtered_file_names.append(full_s3_file)
    except Exception as e:
        logger.error(f"Unknown Exception - {e}")
        return filtered_file_names
    finally:
        logger.info(f"{matching_object_counter} matching objects found")

    return filtered_file_names

def list_all_objects_with_specific_string_in_key(
    client, bucket, prefix, search_string
):
    """Lists all files in the object store where key name contains a specific string

    Parameters:
        client: str, the boto3 client object
        bucket: str, target bucket location
        prefix: str, folder name under which file is to be checked
        search_string: str, string which should be a part of the object name

    Returns:
        Empty list if the objects are not found
        List of objects found
    """
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    logger.info(f"Search string is {search_string}")

    try:
        filtered_file_names = []
        matching_object_counter = 0
        for page in page_iterator:
            for obj in page["Contents"]:
                if obj["Key"].find(search_string) >= 0:
                    #matching_object_counter = matching_object_counter + 1
                    filtered_file_names.append(obj["Key"])
    except Exception as e:
        logger.error(f"Unknown Exception - {e}")
        return filtered_file_names
    finally:
        logger.info(f"{len(filtered_file_names)} matching objects found")

    return filtered_file_names

def get_object_contents(client, bucket, key):
    """Read the specified file from the object store and return its contents.

    Parameters:
        client: str, the boto3 client object
        bucket: str, target bucket location
        key: str, target filename with prefix

    Returns:
        Contents of the specified object in case object is read successfully.
        False in case the object is not found or could not be read.
    """
    try:
        body = client.get_object(Bucket=bucket, Key=key)[
                            "Body"
                        ].read()
        return body
    except Exception as e:
        logging.error(f"Unknown exception - {e}")
        return False

def delete_object(client, bucket, key):
    """Delete the specified file from the object store. The file should have the required prefix (folder path)

    Parameters:
        client: str, the boto3 client object
        bucket: str, target bucket location
        key: str, target filename with prefix

    Returns:
        True in case object is deleted successfully
        False in case the object is not found
    """
    try:
        response = client.delete_object(Bucket=bucket, Key=key)
        logging.debug(f"Response is - {response}")
        if response["ResponseMetadata"].get("HTTPStatusCode") == 204:
            return True
        else:
            logging.error(f"Response - {response}")
            return False
    except Exception as e:
        logging.error(f"Unknown exception - {e}")
        return False
