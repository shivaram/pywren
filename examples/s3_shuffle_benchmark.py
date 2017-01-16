"""
Benchmark aggregate read/write performance to S3

$ python s3_benchmark.py read --bucket_name=jonas-pywren-benchmark   --number=800 --key_file=big_keys.txt

$ python s3_benchmark.py write --bucket_name=jonas-pywren-benchmark   --mb_per_file=1000 --number=800 --key_file=big_keys.txt

Saves the output in a pickled list of IO transfer times. 


Note that you want to write to the root
of an s3 bucket (no prefix) to get maximum
performance as s3 apparently shards
based on the first six chars of the keyname. 

"""

import time
import boto3 
import botocore
import uuid

import time
import pywren
import subprocess
import logging
import sys
import hashlib
import cPickle as pickle
import click
import exampleutils

from collections import deque

@click.group()
def cli():
    pass


@cli.command()
@click.option('--bucket_name', help='bucket to save shuffle files in')
@click.option('--mb_per_file', help='MB of each object in S3', type=int)
@click.option('--workers', help='number of files', type=int)
@click.option('--outfile', default='s3_benchmark.shuffle.output.pickle', 
              help='filename to save results in')
@click.option('--region', default='us-west-2', help="AWS Region")
def shuffle(bucket_name, mb_per_file, workers, 
              outfile, region):

    print "bucket_name =", bucket_name
    print "mb_per_file =", mb_per_file
    print "workers=", workers

    def run_command(my_worker_id):
        bytes_n = mb_per_file * 1024**2

        client = boto3.client('s3', region)
        t1 = time.time()
        for i in xrange(0, workers):
          if i != my_worker_id:
            d = exampleutils.RandomDataGenerator(bytes_n)
            key_name = 'shuffle_' + str(my_worker_id) + '_' + str(i)
            client.put_object(Bucket=bucket_name, 
                              Key = key_name,
                              Body=d)
        t2 = time.time()

        write_mb_rate = bytes_n * (workers - 1)/(t2-t1)/1e6

        # Read in 
        blocksize = 1024*1024
        t3 = time.time()
        bytes_read = 0
        blocks_to_read = list(range(workers))
        blocks_to_read.remove(my_worker_id)
        blocks_to_read = deque(blocks_to_read)

        while blocks_to_read:
          m = hashlib.md5()
          block_id = blocks_to_read.pop()
          key = 'shuffle_' + str(block_id) + '_' + str(my_worker_id)
          try:
            obj = client.get_object(Bucket=bucket_name, Key=key)
            fileobj = obj['Body']
            buf = fileobj.read(blocksize)
            while len(buf) > 0:
              bytes_read += len(buf)
              m.update(buf)
              buf = fileobj.read(blocksize)
          except botocore.exceptions.ClientError as e:
            print 'Failed to get ', block_id, ' error ', e.msg
            blocks_to_read.append(block_id)


        t4 = time.time()
        read_mb_rate = bytes_read/(t4-t3)/1e6
        return t1, t2, write_mb_rate, t3, t4, read_mb_rate

    wrenexec = pywren.default_executor()

    fut = wrenexec.map(run_command, range(workers))

    res = [f.result() for f in fut]
    pickle.dump(res, open(outfile, 'w'))

if __name__ == '__main__':
    cli()
