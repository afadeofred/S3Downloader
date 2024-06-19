# Author: Arun Puram(arun.puram@outlook.com)
# Date: 20/05/2024
# Copyright © 2024  All rights reserved.

import argparse
import boto3
import os
import logging
from datetime import datetime
from mypy_boto3_s3 import S3Client
from rich.progress import Progress
import pytz

def setup(profile: str = None): # type: ignore
    script_dir = os.path.dirname(__file__)
    attachment_list_file_path = os.path.join(script_dir, "attachments.txt")
    download_path = os.path.join(script_dir, "files/")
    
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    
    session = boto3.Session(profile_name=profile)
    client = session.client("s3")       
    return client, attachment_list_file_path, download_path

def download_file_from_s3(s3_client, bucket, attachment_list_file_path, download_dir):
    logging.basicConfig(level=logging.INFO)

    try:
        with open(attachment_list_file_path, "r") as attachment_paths:
            download_tasks = [path.rstrip(os.linesep) for path in attachment_paths]
            
            with Progress() as progress:
                download_task = progress.add_task("[green]Downloading files...", total=len(download_tasks))
                
                for i, partial_filename in enumerate(download_tasks):
                    paginator = s3_client.get_paginator('list_objects_v2')
                    page_iterator = paginator.paginate(Bucket=bucket, Prefix=partial_filename)
                    
                    for page in page_iterator:
                        if 'Contents' in page:
                            for content in page['Contents']:
                                if content['Key'].startswith(partial_filename):
                                    # Extract filename from matched_object early
                                    filename = content['Key'].split('/')[-1]
                                    break
                        if filename:
                            break
                    
                    if filename:
                        # Generate a timezone-aware timestamp
                        local_tz = pytz.timezone('UTC')  # Use UTC for consistency; adjust as needed
                        filename_with_timestamp = f"{filename}_{local_tz.localize(datetime.now()).strftime('%Y%m%d%H%M%S%f')}"
                        
                        download_path = os.path.join(download_dir, filename_with_timestamp)
                        logging.info(f"Downloading {content['Key']} to {download_path}")
                        s3_client.download_file(bucket, content['Key'], download_path)
                        progress.update(download_task, advance=1)
                    else:
                        logging.warning(f"No matching file found for {partial_filename}")

    except FileNotFoundError as e:
        logging.error(f"Error: Attachment list file not found: {e}")
    except ClientError as e:
        logging.error(f"Failed to download a file: {e}")

def main(profile: str, bucket: str):
    client, attachment_list_file_path, download_path = setup(profile)
    download_file_from_s3(client, bucket, attachment_list_file_path, download_path)

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--profile", action="store", type=str, help="Optional AWS profile")
    arg_parser.add_argument("--bucket", action="store", type=str, help="S3 Bucket containing objects")
    args = arg_parser.parse_args()
    main(args.profile, args.bucket)
