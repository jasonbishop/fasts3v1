fasts3v1
========

fast s3 upload version 1

This is version 1 of fasts3.  This includes only upload utility.

The problem statement is:

With increasing number of streams, generally speaking networks get faster.
With increasing number of streams, generally speaking mechanical disks get slower.

Hence the performance curves have opposite slopes.  Where they cross is
the performance you see.

This script attempts to make use of memory buffers to make the storage
curve behave more like the network.

Theory of operation:

Use a single disk stream to fill buffers.  Once a buffer is full, the
multipart upload is kicked off and the disk read is picked up by another
buffer.  As long there is an available buffer then the disk is read
continuously.  Technically the disk is ready by many threads, but some
care is taken to make sure only one thread is reading at any one time.

Use:

To use set your aws credentials in your environment:

export AWS_ACCESS_KEY_ID=ABCDEFGHIJKLMNOPQRST
export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKLMNOPQRSTVUWXYZ

python fasts3upv1.py file bucket
