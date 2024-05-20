# S3Downloader
-- Download files from an S3 bucket

-- This script allows you to download the files hosted on S3 buckets. 
-- You will need to have your AWS profile configured first before running the script.
aws sso login --profile 'PROFILE_NAME'

-- Refer to Attachments.txt for packages. 

pip install -r requirements.txt 
-- To install the required packages before running the sript. 

python s3Downloader.py 
-- To run the script.

More functionalities to come, 
At the moment this script will download objects that are correctly named in the attachments.txt if there are multiple objects with same name you will have to pass in an argument at the folder level inside the bucket.
Use Pagination for example: Refer to s3Dwonloader2.py for help.
