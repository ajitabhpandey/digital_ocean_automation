import os
import logging

from dotenv import load_dotenv


import boto3.session
from botocore.client import Config
from dotenv.main import load_dotenv

from dolib.spaces_operations import is_file_present, upload_to_object_store


def main():

    ALLOWED_EXTENSIONS = (".mp4")
    #ALLOWED_EXTENSIONS = (".flv", ".mp4")
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

    logging.info("Started uploader run...")
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
        # Process each file present in source location
        for filename in os.listdir(source_dir):
            # Consider only FLV and MP4 files
            if filename.endswith(tuple(ALLOWED_EXTENSIONS)):
                # Check if the file already exists and
                # TODO - Check if the size of the source and destination file are matching
                if is_file_present(client, DO_BUCKET, DO_TARGET_FOLDER, filename):
                    logger.info(f"File {filename} already exists, this will be skipped")
                else:
                    logger.info(f"Uploading {os.path.join(source_dir, filename)}")
                    extension = os.path.splitext(filename)[1].lstrip(".")
                    if upload_to_object_store(
                        client,
                        DO_BUCKET,
                        os.path.join(source_dir, filename),
                        DO_TARGET_FOLDER + filename,
                        FILE_CONTENT_TYPES[extension],
                    ):
                        logger.info(f"Uploaded {os.path.join(source_dir, filename)}")
                        try:
                            os.remove(os.path.join(source_dir, filename))
                            logger.info(f"Removed {os.path.join(source_dir, filename)}")
                        except Exception as e:
                            logger.error(
                                f"Exception while removing the file {os.path.join(source_dir, filename)} - {e}"
                            )
                    else:
                        logger.error(
                            f"Error uploading {os.path.join(source_dir, filename)}"
                        )

    finally:
        logger.info("Completed the uploader run...")


if __name__ == "__main__":
    main()
