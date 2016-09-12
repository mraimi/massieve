import botocore
import boto3
s3 = boto3.resource('s3')
buck_name = 'network-traffic'
bucket = s3.Bucket(buck_name)

exists = True
try:
    s3.meta.client.head_bucket(Bucket='mybucket')
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

if exists:
    print 'bucket: %s found ' % buck_name
else:
    print 'Error! Bucket: %s not found ' % buck_name
