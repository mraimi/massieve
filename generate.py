import botocore
import boto3
import sys
import random

s3 = boto3.resource('s3')
buck_name = 'network-traffic'
bucket = s3.Bucket(buck_name)

exists = True
try:
    s3.meta.client.head_bucket(Bucket=buck_name)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

if exists:
    print 'bucket: %s found ' % buck_name
else:
    sys.exit('Error! Bucket: %s not found ' % buck_name)

for i in xrange(1,8):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    for day in days:
        dir = '/home/ec2-user/Week' + str(i) + '/' + day + '/gureKddcup-matched.list'
        try:
            records = open(dir, 'r')
        except IOError:
            print("file at: \n\t" + dir + "\n not found")
            continue
        for record in records:
            spl = record.strip().split(' ')
            print spl

            random.seed()

            # Gently perturb data with small modifications
            try:
                for i in xrange(30, 47):
                    if i == 37 or i == 38:
                        spl[i] = max(0, int(spl[i])+random.randint(-1,1))
                    else:
                        spl[i] = float(spl[i]) + float(random.randint(0, 9))/100000.0
            except Exception:
                print 'Malformed record aborted.'
                continue

            print spl
            sys.exit()
