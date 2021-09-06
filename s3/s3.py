import boto3
import logging
from botocore.exceptions import ClientError

def create_bucket(bucket,region):

    try:
        if  valid_bucket_name(bucket) and valid_region(region):
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket,CreateBucketConfiguration={'LocationConstraint': region})
    except ClientError as e:
        logging.error(e)
        return False
    return True


def valid_bucket_name(bucket):
    if bucket is not None and bucket != "" and len(bucket) >= 3 and len(bucket) <=63:   
        return True
    else:
        return False

def valid_region(region):
    if region is not None and region !="":
        return True
    else:
        return False

def valid_object(name):
    if name is not None and name !="":
        return True
    else:
        return False

def get_buckets():

    session = boto3.session.Session()
    s3_client = session.client('s3')
    buckets =[]

    try:

       response = s3_client.list_buckets()

       for bucket in response['Buckets']:
           buckets += {bucket["Name"]}

    except ClientError:
        print("Couldn't get buckets.")
    return buckets

def get_files(bucket):
    
    try:
        files = []

        if valid_bucket_name(bucket):
            
            s3 = boto3.resource('s3')
            my_bucket = s3.Bucket(bucket)
            
            for my_bucket_object in my_bucket.objects.all():
                files.append(my_bucket_object.key)
        else:
            raise ValueError("The name of the bucket is empty or does not meet the naming conventions")

        return files

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            error = "The bucket requested does not exist!"
            return error

def upload_file_to_bucket(file_name,bucket,object_name):

    try:
        if valid_bucket_name(bucket) and valid_object(file_name) and valid_object(file_name):
            s3 = boto3.resource('s3')    
            s3.Bucket(bucket).upload_file(file_name,object_name)
    except ClientError as e:
        print("The file could not be uploaded, check if the file exist or if the file has read only permission")
        logging.error(e)

