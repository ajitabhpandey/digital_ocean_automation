import logging

logger = logging.getLogger(__name__)


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


def upload_to_object_store(client, bucket, full_path_to_filename, object_name=None):
    """Uploads the specified file to the object store

    Parameters:
        client: str, the boto3 client object
        bucket: str, target bucket location
        full_path_to_filename: str, full path of the file to be uploaded
        object_name: str, the new name of the file at the target location. full_path_to_filename is used if not specified

    Returns:
        False if the upload fails
        True if the upload succeeds
    """
    try:
        # Upload the file to Spaces
        client.upload_file(full_path_to_filename, bucket, object_name)
    except Exception as e:
        logger.error(f"Exception while uploading file - {e}")
        return False
    else:
        return True
