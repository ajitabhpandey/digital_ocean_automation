import os
import logging
from timeit import default_timer as timer
from datetime import timedelta
import argparse
from dotenv import load_dotenv

import boto3.session
from dotenv.main import load_dotenv

import pandas as pd
import io

from dolib.spaces_operations import (
    list_all_objects_with_specific_string_in_key,
    get_object_contents
)


def get_arguments():
    """Concat all CSVs in a folder in Digital Ocean Spaces Object Store and stores content into a single CSV"""
    parser = argparse.ArgumentParser()
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
        dest="source_folder",
        required=True,
        help="Folder inside a bucket where the files are to be searched",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        dest="output_folder",
        required=True,
        help="Folder relative to the specified base bucket where the merged file will be stored",
    )

    parser.add_argument(
        "-s",
        "--suffix",
        type=str,
        dest="suffix",
        default=".csv",
        help="Suffix of files to include in the combination",
    )

    parser.add_argument(
        "-e",
        "--event",
        type=str,
        dest="event_id",
        required=True,
        help="Suffix of files to include in the combination",
    )

    options = parser.parse_args()
    return options

def get_logger(log_file):
    # log_encoding = "utf-8"
    loglevel = "INFO"
    logging.basicConfig(
        format="%(asctime)s - [%(name)s] - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S %p",
        filename=log_file,
        # encoding=log_encoding,
        level=getattr(logging, loglevel.upper()),
    )
    return logging.getLogger(__name__)

def new_s3_client():
    # initialize an S3 client with a private session so that multithreading
    # doesn't cause issues with the client's internal state
    try:
        session = boto3.session.Session()
        return session.client(
            "s3",
            region_name=DO_REGION,
            endpoint_url=DO_SPACES_URL,
            aws_access_key_id=DO_ACCESS_ID,
            aws_secret_access_key=DO_SECRET_KEY,
        )
    except Exception as e:
        logger.error(f"Error while initiating session - {e}")

def main(bucket, folder, event_id):

    s3_client = new_s3_client()
    df = []
    csv_buffer = io.StringIO()
    object_list = list_all_objects_with_specific_string_in_key(s3_client, bucket, folder, event_id) 
    if len(object_list) > 0:
        for object in object_list:
            body = get_object_contents(s3_client, bucket, object)
            temp = pd.read_csv(
                io.BytesIO(body), encoding="utf-8", index_col=None, header=0
            )
            df.append(temp)
        frame = pd.concat(df, axis=0, ignore_index=True)
        frame.to_csv(csv_buffer, header=True, index=False)
        csv_buffer.seek(0)
        consolidated_data_file = output_folder + event_id + '.' + suffix
        
        s3_client.put_object(
            Bucket=bucket,
            Body=csv_buffer.getvalue(),
            Key=consolidated_data_file,
        )
        logger.info(f"Saved consolidated data to {consolidated_data_file}")
    else:
        logger.info("The object list is empty. Nothing to process")

if __name__ == "__main__":
    # Measuring time
    start_time = timer()

    options = get_arguments()
    source_folder = options.source_folder
    output_folder = options.output_folder
    suffix = options.suffix
    event_id = options.event_id
    bucket = options.bucket

    # setting up logger
    LOG_FILE = "do_spaces_concat_files.log"
    logger = get_logger(LOG_FILE)

    logger.info("Started the concat and join...")

    # Ensure that the user has not supplied the empty string as a bucket as that is invalid
    if bucket and bucket.isspace():
        logger.error("Bucket name has to be specified")
        exit(1)

    # Add a trailing slash to the DO_TARGET_FOLDER if it does not exists
    if not source_folder.endswith("/"):
        logger.info("Missing trailing slash from source_folder, adding one")
        source_folder = source_folder + "/"

    if not output_folder.endswith("/"):
        logger.info("Missing trailing slash from output_folder, adding one")
        output_folder = output_folder + "/"

    # take environment variables from .env
    load_dotenv()

    DO_ACCESS_ID = os.getenv("DO_ACCESS_ID")
    DO_SECRET_KEY = os.getenv("DO_SECRET_KEY")
    DO_REGION = os.getenv("DO_REGION")
    DO_SPACES_URL = f"https://{DO_REGION}.digitaloceanspaces.com"

    main(bucket, source_folder, event_id)

    end_time = timer()
    logger.info(
        f"Total Time taken for processing {timedelta(seconds=end_time-start_time)} seconds"
    )
    logger.info("Completed the concat and join run...")
