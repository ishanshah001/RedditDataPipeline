import s3fs
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY
import logging

def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                               key= AWS_ACCESS_KEY_ID,
                               secret=AWS_ACCESS_KEY,
                               s3_additional_kwargs={'ACL': 'private'})
        return s3
    except Exception as e:
        print(e)

def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
    try:
        if not s3.exists(bucket):
            # print(bucket)
            s3.mkdir(bucket)
            print("Bucket created")
        else :
            print("Bucket already exists")
    except Exception as e:
        print(e)


def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket:str, s3_file_name: str):
    try:
        s3.put(file_path, bucket+'/raw/'+ s3_file_name, acl='private')
        print('File uploaded to s3')
    except FileNotFoundError:
        logging.error('The file was not found: %s', file_path)
    except PermissionError:
        logging.error('Access denied while uploading to S3 bucket: %s', bucket)
    except Exception as e:
        logging.error('An unexpected error occurred: %s', str(e))