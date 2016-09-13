#!/usr/bin/python

import botocore
import boto3
import sys
import random
import datetime


def log(logfile, msg):
    logfile.write("[" + str(datetime.datetime.utcnow()) + "] " + msg)

if len(sys.argv) < 2:
    sys.exit("Usage: \n\t python generate.py <duplication_factor> \n")
else:
    dupl_factor = int(sys.argv[1])

s3 = boto3.resource('s3')
buck_name = 'network-traffic'
bucket = s3.Bucket(buck_name)
lg = open('generation.log', 'a')

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
    log(lg, 'bucket: %s found ' % buck_name + "\n")
else:
    log(lg, 'Error! Bucket: %s not found ' % buck_name)
    lg.close()
    sys.exit('Error! Bucket: %s not found ' % buck_name)

try:
    for i in xrange(1,8):
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        week = 'Week' + str(i)
        log(lg, 'Starting week: ' + week + '...' + "\n")
        random.seed()

        for day in days:
            obj_name = week + '-' + day
            s3obj = None
            try:
                s3obj = s3.Object(buck_name, obj_name)
            except Exception:
                log(lg, 'Failed to create s3 object, ' + obj_name + '.')
                lg.close()
                sys.exit('Failed to create s3 object, ' + obj_name + '.')
            expanded_file = None
            path = '/home/ec2-user/data/' + week + '-' + day
            try:
                expanded_file = open(path, 'w')
            except IOError:
                log(lg, 'Could not create a file to generate data to.')
                lg.close()
                sys.exit('Could not create a file to generate data to.')

            log(lg, 'Starting day: ' + day + '...' + "\n")
            day_path = '/home/ec2-user/' + week + '/' + day + '/gureKddcup-matched.list'
            records = None
            try:
                records = open(day_path, 'r')
            except IOError:
                log(lg, "file at: \n\t" + day_path + "\n not found. Skipping... \n")
                continue
            for record in records:
                for j in xrange(0, dupl_factor):
                    spl = record.strip().split(' ')
                    to_send = ''

                    # Gently perturb data with small modifications
                    try:
                        for k in xrange(30, 47):
                            if k == 37 or k == 38:
                                spl[k] = max(0, int(spl[k])+random.randint(-1, 1))
                            else:
                                spl[k] = float(spl[k]) + float(random.randint(0, 9))/1000000.0
                    except Exception:
                        log(lg, 'Malformed record aborted.' + "\n")
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
            log(lg, 'Ending day: ' + day + '...' + "\n")
            s3obj.put(Body=open(path, 'r'))
            log(lg, 'Sent ' + week + '-' + day + ' to s3.')
        log(lg, 'Ending week: ' + week + '...' + "\n")
except Exception:
    log(lg, 'Exception caught at top level. Exiting.')
    lg.close()
lg.close()
