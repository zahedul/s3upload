import logging
import uuid

import boto3
from boto3 import session
import requests
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s:%(lineno)s] %(message)s"
)
logger = logging.getLogger(__name__)


BUCKET_NAME = "zahed-test"
AWS_PROFILE = "dazntest"


def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    session = boto3.session.Session(profile_name=AWS_PROFILE)
    s3_client = session.client('s3')

    try:
        params = {'Bucket': bucket_name, 'Key': object_name}
        response = s3_client.generate_presigned_url(
            'put_object',
            Params=params,
            ExpiresIn=expiration
        )
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response


def create_presigned_post(bucket_name, object_name,
                          fields=None, conditions=None, expiration=3600):
    """Generate a presigned URL S3 POST request to upload a file

    :param bucket_name: string
    :param object_name: string
    :param fields: Dictionary of prefilled form fields
    :param conditions: List of conditions to include in the policy
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Dictionary with the following keys:
        url: URL to post to
        fields: Dictionary of form fields and values to submit with the POST
    :return: None if error.
    """

    # Generate a presigned S3 POST URL
    session = boto3.session.Session(profile_name=AWS_PROFILE)
    s3_client = session.client('s3')
    try:
        response = s3_client.generate_presigned_post(bucket_name,
                                                     object_name,
                                                     Fields=fields,
                                                     Conditions=conditions,
                                                     ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL and required fields
    return response


def s3_upload_with_post_url():
    # Generate a presigned S3 POST URL
    object_name = str(uuid.uuid4().hex[:6]) + '_presigned_post.txt'
    filename = '100mb_file.txt'
    response = create_presigned_post(BUCKET_NAME, object_name)
    if response is None:
        exit(1)
        
    logger.info(response)

    # Demonstrate how another Python program can use the presigned URL to upload a file
    with open(filename, 'rb') as f:
        files = {'file': (object_name, f)}
        http_response = requests.post(response['url'], data=response['fields'], files=files)
    # If successful, returns HTTP status code 204
    logger.info(f'File upload HTTP status code: {http_response.status_code}')
    

def get_list():
    session = boto3.session.Session(profile_name=AWS_PROFILE)
    s3 = session.resource('s3')
    
    my_bucket = s3.Bucket(BUCKET_NAME)
    for file in my_bucket.objects.all():
        print(file.key)


def simple_upload():
    session = boto3.session.Session(profile_name=AWS_PROFILE)
    s3 = session.resource('s3')
    
    file_name = create_temp_file(100*1000*1024, 'file.txt', 't')
    
    s3_object = s3.Object(BUCKET_NAME, file_name)
    try:
        s3_object.upload_file(file_name)
        logger.info('file uploaded')
    except Exception as e:
        logger.error(e)


def upload_presigned_url():
    keyname = str(uuid.uuid4().hex[:6]) + '_presigned.txt'
    filename = "2mb_file.txt"
    url = create_presigned_url(BUCKET_NAME, keyname)
    
    headers = {'Slug': filename}

    try:
        if url is not None:
            response = requests.put(url, data=open(filename, 'rb'), headers=headers)
            response.raise_for_status()
            logger.info(response.status_code)
            logger.info(response.text)
    except Exception as e:
        logger.error(e)
        

def create_post_curl_request():
    object_name = str(uuid.uuid4().hex[:6]) + '_curl_post.txt'
    filename = '2mb_file.txt'
    response = create_presigned_post(BUCKET_NAME, object_name)
    if response is None:
        exit(1)

    url = response['url']
    fields = response['fields']

    field_params = []
    for f in fields.keys():
        field_params.append(f"-F '{f}={fields[f]}'")

    post_form_params = " ".join(field_params)
    
    logger.info(url)
    logger.info(post_form_params)
    
    with open("curl_upload.sh", "w") as f:
        f.write("curl -i -v ")
        f.write(post_form_params)
        f.write(f" -F 'file=@{filename}' ")
        f.write(url)


def create_curl_request():
    object_name = str(uuid.uuid4().hex[:6]) + '_curl_put.txt'
    filename = '2mb_file.txt'
    url = create_presigned_url(BUCKET_NAME, object_name)
    if url is None:
        exit(1)
        
    logger.info(url)

    with open("curl_upload.sh", "w") as f:
        f.write("curl -i -v -include ")
        f.write(f"-T ./{filename} ")
        f.write(f"'{url}'")


if __name__ == '__main__':
    create_post_curl_request()
