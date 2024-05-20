#s3Downloader 2.0

import argparse
import boto3
import os
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.paginator import Paginator


def setup(profile: str = None):
    

    script_dir = os.path.dirname(__file__)
    attachment_list_file_path = os.path.join(script_dir, "attachments.txt")
    download_path = os.path.join(script_dir, "files/")

    # Create File Directory
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Setup AWS client
    session = boto3.Session(profile_name=profile)
    client = session.client("s3")

    return client, attachment_list_file_path, download_path

    #Download files from S3 bucket Using Paginator to iterate through the entire the bucket 
def download_file_from_s3(
    s3: S3Client, bucket: str, attachment_list_file_path: str, download_path: str, download_all_in_folder: str = None
):

    with open(attachment_list_file_path, "r") as attachment_paths:
        paginator = Paginator(s3, "list_objects_v2", Bucket=bucket, Prefix=download_all_in_folder + "/")  # Use pagination
        for page in paginator.paginate():
            for item in page.get("Contents", []):
                filename = item["Key"].rpartition("/")[-1]
                local_file_path = os.path.join(download_path, filename.rstrip(os.linesep))

                # Check if download_all_in_folder is set and the path starts with the folder name
                if download_all_in_folder and item["Key"].startswith(f"{download_all_in_folder}/"):
                    # Download the file regardless of duplicate names
                    s3.download_file(bucket, item["Key"], local_file_path)
                else:
                    # Existing logic for handling potential duplicate names (use counter if not in folder)
                    counter = 1
                    while os.path.exists(local_file_path):
                        name, ext = os.path.splitext(filename)
                        new_filename = f"{name}_{counter}{ext}"
                        local_file_path = os.path.join(download_path, new_filename)
                        counter += 1
                    s3.download_file(bucket, item["Key"], local_file_path)


def main(profile: str, bucket: str, download_all_in_folder: str = None):
    

    client, attachment_list_file_path, download_path = setup(profile)
    download_file_from_s3(client, bucket, attachment_list_file_path, download_path, download_all_in_folder)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--profile", action="store", type=str, help="Optional AWS profile"
    )
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--bucket", action="store", type=str, help="Optional AWS profile"
    )
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--downlaod-all-in-folder", action="store", type=str, help="Optional AWS profile"
    )
