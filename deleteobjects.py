import os
import logging
from datetime import timezone, timedelta
import datetime
import argparse
from dotenv import load_dotenv

from dolib.spaces_operations import (
    list_all_objects_older_than_last_modified,
    new_s3_client,
)


def get_arguments():
    """Delete Objects from Digital Ocean Spaces Object Store"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--datetime",
        type=str,
        dest="utc_datetime",
        default=datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        help="Date and Time in UTC. Default is current date and time.",
    )
    parser.add_argument(
        "-n",
        "--num",
        type=int,
        dest="num_days_before",
        default=14,
        help="Number of days before the specified date-time. Default is 14 days.",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        type=str,
        dest="bucket",
        required=True,
        help="Bucket where the objects are to be searched",
    )
    parser.add_argument(
        "-f",
        "--folder",
        type=str,
        dest="folder",
        required=True,
        help="Folder inside a bucket where the files are to be searched",
    )
    options = parser.parse_args()
    return options


def main(utc_datetime, num_days, bucket, folder):
    # take environment variables from .env
    load_dotenv()

    DO_ACCESS_ID = os.getenv("DO_ACCESS_ID")
    DO_SECRET_KEY = os.getenv("DO_SECRET_KEY")
    DO_REGION = os.getenv("DO_REGION")
    DO_SPACES_URL = f"https://{DO_REGION}.digitaloceanspaces.com"

    # Use provided bucket if valid, otherwise fall back to env variable
    if bucket and not bucket.isspace():
        DO_BUCKET = bucket
    else:
        DO_BUCKET = os.getenv("DO_BUCKET")

    DO_TARGET_FOLDER = folder

    log_file = "do_spaces_delete.log"
    loglevel = "INFO"
    logging.basicConfig(
        format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S %p",
        filename=log_file,
        level=getattr(logging, loglevel.upper()),
    )
    logger = logging.getLogger(__name__)
    
    # Also add console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %I:%M:%S %p")
    console_handler.setFormatter(formatter)
    logging.getLogger().addHandler(console_handler)

    delete_counter = 0  # Initialize outside try block to avoid NameError
    logging.info("Started deletion...")
    
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
        target_utc_datetime = utc_datetime - timedelta(days=num_days)
        logger.info(
            f"Specified UTC Date is {utc_datetime}, Target UTC Date is {target_utc_datetime}"
        )
        object_list = list_all_objects_older_than_last_modified(
            client, DO_BUCKET, DO_TARGET_FOLDER, target_utc_datetime
        )
        
        if object_list:
            logger.info(f"{len(object_list)} objects received")
            
            # Batch delete objects - much more efficient than one-by-one deletion
            # AWS allows up to 1000 objects per delete_objects call
            for i in range(0, len(object_list), 1000):
                batch = [
                    {'Key': obj} 
                    for obj in object_list[i:i+1000] 
                    if not obj.endswith("/")
                ]
                
                if batch:
                    try:
                        response = client.delete_objects(
                            Bucket=DO_BUCKET,
                            Delete={'Objects': batch}
                        )
                        
                        # Count successfully deleted objects
                        if 'Deleted' in response:
                            batch_deleted = len(response['Deleted'])
                            delete_counter += batch_deleted
                            logger.info(f"Batch deleted {batch_deleted} objects")
                        
                        # Log any deletion errors
                        if 'Errors' in response:
                            for error in response['Errors']:
                                logger.error(
                                    f"Failed to delete {error['Key']}: {error['Message']}"
                                )
                    except Exception as e:
                        logger.error(f"Error during batch deletion - {e}")
                        return False
        else:
            logger.warning(
                f"0 objects received. No objects older than {target_utc_datetime} found."
            )

    except Exception as e:
        logger.error(f"Unexpected error during deletion - {e}")
        return False
    
    finally:
        logger.info(f"Successfully deleted {delete_counter} objects")
        logger.info("Completed the deletion run...")
    
    return True


if __name__ == "__main__":
    options = get_arguments()
    main(
        datetime.datetime.strptime(options.utc_datetime, "%Y-%m-%d %H:%M:%S"),
        options.num_days_before,
        options.bucket,
        options.folder,
    )
