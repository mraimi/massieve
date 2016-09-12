#!/usr/bin/python

import botocore
import boto3
import sys
import random

if len(sys.argv) < 2:
    sys.exit("Usage: \n\t python generate.py <duplication_factor>")
else:
    dupl_factor = sys.argv[1]

s3 = boto3.resource('s3')
buck_name = 'network-traffic'
bucket = s3.Bucket(buck_name)
log = open('generation.log', 'a')

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
    log.write('bucket: %s found ' % buck_name)
else:
    sys.exit('Error! Bucket: %s not found ' % buck_name)

for i in xrange(1,8):
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    week = 'Week' + str(i)
    log.write('Starting week: ' + week + '...')

    try:
        expanded_file = open('/home/ec2-user/data/' + week, 'a')
    except IOError:
        sys.exit('Could not create a file to generate data to.')

    try:
        s3obj = s3.Object(buck_name, week)
    except Exception:
        sys.exit('Failed to create s3 object. Check your bucket name.')

    for day in days:
        log.write('Starting day: ' + day + '...')
        dir = '/home/ec2-user/' + week + '/' + day + '/gureKddcup-matched.list'

        try:
            records = open(dir, 'r')
        except IOError:
            log.write("file at: \n\t" + dir + "\n not found")
            continue
        for record in records:
            random.seed()

            for j in xrange(0, dupl_factor):
                spl = record.strip().split(' ')
                to_send = ''

                # Gently perturb data with small modifications
                try:
                    for k in xrange(30, 47):
                        if k == 37 or k == 38:
                            spl[k] = max(0, int(spl[k])+random.randint(-1,1))
                        else:
                            spl[k] = float(spl[k]) + float(random.randint(0, 9))/1000000.0
                except Exception:
                    log.write('Malformed record aborted.')
                    continue

                for field in spl:
                    if isinstance(field, float):
                        to_send += ' ' + "%.6f" % field
                    else:
                        to_send = to_send + ' ' + str(field)

                to_send = to_send.strip()
                to_send += "\n"
                expanded_file.write(to_send)
            expanded_file.close()
            s3obj.put(Body=open('/home/ec2-user/data/' + week, 'rb'))
            sys.exit()
    log.write('Ending week: ' + week + '...')
    expanded_file.close()
    s3obj.put(Body=open('/home/ec2-user/data/' + week, 'rb'))
log.close()
