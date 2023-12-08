import os
import boto3

## Update these before running ##
DOWNLOAD_PATH = 'files/'
AWS_PROFILE_NAME = 'AWS_PROFILE_NAME'
ATTACHMENT_LIST_FILE = 'attachments.txt'
S3_BUCKET = 'BUCKET_NAME'

## creating a session ##
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
s3 = session.client('s3')

def download_file_from_s3():
    with open(ATTACHMENT_LIST_FILE, 'r') as attachment_paths:
        for attachment_path in attachment_paths:
            filename = attachment_path.rpartition('/')[-1]
            
            dest_pathname = DOWNLOAD_PATH + '/' +filename
            print(dest_pathname)
            s3.download_file(S3_BUCKET, attachment_path.rstrip(os.linesep), dest_pathname.encode('unicode-escape').decode().replace("\\",""))

if __name__ == "__main__":
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)
    
    download_file_from_s3()
