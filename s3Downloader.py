# Author: Arun Puram(arun.puram@outlook.com)
# Date: 20/05/2024
# Copyright © 2024  All rights reserved.

import argparse
import boto3
import os
from mypy_boto3_s3 import S3Client
import logging
from rich.progress import Progress

def setup(profile: str = None):

    script_dir = os.path.dirname(__file__)
    attachment_list_file_path = os.path.join(script_dir, "attachments.txt")
    download_path = os.path.join(script_dir, "files/")

    # Create download directory if it doesn't exist
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Setup AWS client
    session = boto3.Session(profile_name=profile)
    client = session.client("s3")

    return client, attachment_list_file_path, download_path


def download_file_from_s3(
    s3: S3Client, bucket: str, attachment_list_file_path: str, download_dir: str
):

    logging.basicConfig(level=logging.INFO)  # Configure logging
               
    try: #Error Handling
        with open(attachment_list_file_path, "r") as attachment_paths:
            download_tasks = [path.rstrip(os.linesep) for path in attachment_paths]  # List of download tasks

            with Progress() as progress:
                download_task = progress.add_task("[green]Downloading files...", total=len(download_tasks))
                for i, attachment_path in enumerate(download_tasks):
                    filename = attachment_path.rpartition("/")[-1]
                    download_path = os.path.join(download_dir, filename)
                    logging.info(f"Downloading {attachment_path} to {download_path}")
                    s3.download_file(bucket, attachment_path, download_path)

                    # Update progress bar
                    progress.update(download_task, advance=1)  # Update progress for each downloaded file

    except FileNotFoundError as e:
        logging.error(f"Error: Attachment list file not found: {e}")
        raise #raise an error if file not found


def main(profile: str, bucket: str):
    client, attachment_list_file_path, download_path = setup(profile)
    download_file_from_s3(client, bucket, attachment_list_file_path, download_path)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--profile", action="store", type=str, help="Optional AWS profile"
    )
    arg_parser.add_argument(
        "--bucket", action="store", type=str, help="S3 Bucket containing objects"
    )
    args = arg_parser.parse_args()
    main(args.profile, args.bucket)
