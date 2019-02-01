#!/usr/bin/python3
# -*- coding: UTF-8 -*-

# Copyright 2019 Hyde. All Rights Reserved.
#
# This file is licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License. A copy of the
# License is located at
#
# http://aws.amazon.com/apache2.0/
#
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import configparser
import getopt
import os
import sys

import boto3

configFile = 'default.cfg'


def check_env():
    aws_key_file = os.path.expanduser('~') + '/.aws/credentials'
    aws_key_file = os.path.abspath(aws_key_file)
    if (os.path.exists(aws_key_file)):
        return True
    else:
        print('~/.aws/credentials not exist')


def load_parms(section_name='aws'):
    parms = {}
    cf = configparser.ConfigParser()
    cf.read(configFile)
    if section_name in cf.sections():
        parms0 = cf.items(section_name)
        for parm in parms0:
            parms[parm[0]] = parm[1]
    return parms


def upload_file(parms):
    bucket_name = parms['bucket_name']
    prefix = parms['prefix']
    filename = parms['upfile']
    check = parms.get('check', 'False')
    # Create an S3 client
    s3 = boto3.client('s3')
    # Call S3 to list current buckets
    if check == 'True':
        response = s3.list_buckets()
        print(response)
        # Get a list of all bucket names from the response
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        # Print out the bucket list
        dir_info = None
        print("Bucket List: %s" % buckets)
        if bucket_name in buckets:
            print('begin')
            dir_info = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=2, Prefix=prefix)
            print(dir_info)
            if (dir_info['ResponseMetadata']['HTTPStatusCode'] == 200):
                print('get file object ok')
            else:
                print('get file failed')
                return
        else:
            print('bucket %s not exist' % (bucket_name))
            return
        print(dir_info['Contents'])
        for fileinfo in dir_info['Contents']:
            print(fileinfo['Key'])
    s3resource = boto3.resource('s3')
    prefix = prefix.rstrip('/')
    s3resource.meta.client.upload_file(filename, bucket_name, prefix + '/' + os.path.basename(filename))


def help():
    msg = '%s -u <filename>' % (sys.argv[0])
    print(msg)


def get_opts(parms):
    argv = sys.argv[1:]
    success = False
    try:
        opts, args = getopt.getopt(argv, "hu:", ['--upfile'])
    except getopt.GetoptError:
        help()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            help()
            sys.exit()
        elif opt in ("-u", "--upfile"):
            parms['upfile'] = arg
            success = True
    if success:
        return parms
    else:
        help()
        sys.exit()


if __name__ == '__main__':
    if not check_env():
        sys.exit(-1)
    parms = load_parms('aws')
    parms = get_opts(parms)
    print(parms)
    upload_file(parms)
