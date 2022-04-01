import os
import logging
from datetime import timezone, timedelta
import datetime
import argparse
from dotenv import load_dotenv

import boto3.session
from botocore.client import Config
from dotenv.main import load_dotenv

from dolib.spaces_operations import (
    list_all_objects_older_than_last_modified,
    delete_object,
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

    if not (bucket and not bucket.isspace()):
        DO_BUCKET = bucket
    else:
        DO_BUCKET = os.getenv("DO_BUCKET")

    DO_TARGET_FOLDER = folder

    log_file = "do_spaces_delete.log"
    # log_encoding = "utf-8"
    loglevel = "INFO"
    logging.basicConfig(
        format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S %p",
        filename=log_file,
        # encoding=log_encoding,
        level=getattr(logging, loglevel.upper()),
    )
    logger = logging.getLogger(__name__)

    logging.info("Started deletion...")
    # Add a trailing slash to the DO_TARGET_FOLDER if it does not exists
    if not DO_TARGET_FOLDER.endswith("/"):
        logger.info("Missing trailing slash from DO_TARGET_FOLDER, adding one")
        DO_TARGET_FOLDER = DO_TARGET_FOLDER + "/"

    try:
        # Initiate Session
        session = boto3.session.Session()
        client = session.client(
            "s3",
            region_name=DO_REGION,
            endpoint_url=DO_SPACES_URL,
            aws_access_key_id=DO_ACCESS_ID,
            aws_secret_access_key=DO_SECRET_KEY,
        )
    except Exception as e:
        logger.error(f"Error while initiating session - {e}")
    else:
        target_utc_datetime = utc_datetime - timedelta(
            num_days
        )  # This means num_days back
        logger.info(
            f"Specified UTC Date is {utc_datetime}, Target UTC Date is {target_utc_datetime}"
        )
        object_list = list_all_objects_older_than_last_modified(
            client, DO_BUCKET, DO_TARGET_FOLDER, target_utc_datetime
        )
        if object_list:
            logger.info(f"{len(object_list)} objects received")
            delete_counter = 0
            for object in object_list:
                if not object.endswith("/") and delete_object(client, bucket, object):
                    logger.debug(f"Object {object} deleted successfully")
                    delete_counter = delete_counter + 1
                else:
                    logger.error(f"Could not delete object {object}")
        else:
            logger.error(
                f"{len(object_list)} objects received. Something is not right."
            )
            return False

    finally:
        logger.info(f"Deleted {delete_counter} objects successfully")
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
