import os
import logging

from dotenv import load_dotenv
from dolib.spaces_operations import new_s3_client


def main():

    ALLOWED_EXTENSIONS = (".mp4",)
    FILE_CONTENT_TYPES = {"mp4": "video/mpeg", "flv": "video/x-flv"}

    # take environment variables from .env
    load_dotenv()

    DO_ACCESS_ID = os.getenv("DO_ACCESS_ID")
    DO_SECRET_KEY = os.getenv("DO_SECRET_KEY")
    DO_REGION = os.getenv("DO_REGION")
    DO_SPACES_URL = f"https://{DO_REGION}.digitaloceanspaces.com"
    DO_BUCKET = os.getenv("DO_BUCKET")
    DO_TARGET_FOLDER = os.getenv("DO_TARGET_FOLDER")
    source_dir = os.getenv("LOCAL_SOURCE_DIR")

    log_file = "do_spaces_uploader.log"
    loglevel = "INFO"
    logging.basicConfig(
        format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S %p",
        filename=log_file,
        level=getattr(logging, loglevel.upper()),
    )
    logger = logging.getLogger(__name__)

    logging.info("Started uploader run...")
    
    # Add a trailing slash to the DO_TARGET_FOLDER if it does not exists
    if not DO_TARGET_FOLDER.endswith("/"):
        logger.info("Missing trailing slash from DO_TARGET_FOLDER, adding one")
        DO_TARGET_FOLDER = DO_TARGET_FOLDER + "/"

    try:
        # Use helper function to instantiate S3 client with retry logic
        client = new_s3_client(
            DO_REGION,
            DO_SPACES_URL,
            DO_ACCESS_ID,
            DO_SECRET_KEY
        )
        
        if not client:
            logger.error("Failed to create S3 client")
            return False
        
    except Exception as e:
        logger.error(f"Error while initiating session - {e}")
        return False

    try:
        # Cache remote file list once - much more efficient than per-file checks
        remote_files = set()
        
        try:
            paginator = client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=DO_BUCKET, Prefix=DO_TARGET_FOLDER)
            
            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        # Extract filename from full key path
                        filename = obj["Key"].replace(DO_TARGET_FOLDER, "")
                        if filename:  # Ignore folder entries
                            remote_files.add(filename)
            
            logger.info(f"Found {len(remote_files)} existing files in remote bucket")
        except Exception as e:
            logger.error(f"Error listing remote files - {e}")
            return False

        # Process each file present in source location
        upload_count = 0
        skip_count = 0
        
        for filename in os.listdir(source_dir):
            # Consider only allowed file extensions
            if filename.endswith(ALLOWED_EXTENSIONS):
                local_path = os.path.join(source_dir, filename)
                
                # Check if file already exists in remote bucket
                if filename in remote_files:
                    logger.info(f"File {filename} already exists, skipping")
                    skip_count += 1
                else:
                    try:
                        logger.info(f"Uploading {local_path}")
                        extension = os.path.splitext(filename)[1].lstrip(".")
                        content_type = FILE_CONTENT_TYPES.get(extension, "binary/octet-stream")
                        
                        # Upload file
                        client.upload_file(
                            local_path,
                            DO_BUCKET,
                            DO_TARGET_FOLDER + filename,
                            ExtraArgs={"ACL": "private", "ContentType": content_type},
                        )
                        logger.info(f"Successfully uploaded {filename}")
                        upload_count += 1
                        
                        # Remove local file after successful upload
                        try:
                            os.remove(local_path)
                            logger.info(f"Removed local file {local_path}")
                        except Exception as e:
                            logger.error(
                                f"Exception while removing local file {local_path} - {e}"
                            )
                    except Exception as e:
                        logger.error(f"Error uploading {filename} - {e}")
        
        logger.info(
            f"Upload complete: {upload_count} uploaded, {skip_count} skipped"
        )
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error during upload - {e}")
        return False
    
    finally:
        logger.info("Completed the uploader run...")


if __name__ == "__main__":
    main()
