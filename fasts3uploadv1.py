import threading, Queue
import os, io, datetime
import boto
from boto.s3.connection import Location
import sys


#s3endpoint = 's3-us-west-2.amazonaws.com'
s3endpoint = 's3.amazonaws.com'

chunk = 10000*4096

# job queue
chunkqueue = Queue.Queue()
needmorequeue = Queue.Queue()

result = []
timewasted = []


class Worker(threading.Thread):
    def __init__(self, num, mp_bucket, f, mp_keyname, mp_id, *args, **kwargs):
        self.mynumber = num
        self.data = bytearray(chunk)
        self.f = f

        conn = boto.connect_s3(host=s3endpoint,
                               is_secure=True,
                               debug=False)

        bucket = conn.lookup(mp_bucket)
        self.mp = boto.s3.multipart.MultiPartUpload(bucket)
        self.mp.key_name = mp_keyname
        self.mp.id = mp_id
        self.mp.bucket_name = bucket_name
        threading.Thread.__init__(self)


    def readdata(self, uploadindex, timeenqueued, (begin, length)):
        metime = datetime.datetime.now()
        differential = (metime-timeenqueued).seconds + (metime-timeenqueued).microseconds/1e6
        timewasted.append(differential)
        self.f.seek(begin)
        bytesread = self.f.readinto(self.data)
        if bytesread < chunk:
            self.data = bytearray(self.data[:bytesread])
        return '%d bytes read %d to %d' % (bytesread, begin, bytesread+begin)


    def dos3upload(self, uploadindex, timeenqueued, (begin, mylength)):
        self.mp.upload_part_from_file(io.BytesIO(self.data), uploadindex+1)

    def run(self):
        while 1:
            args = chunkqueue.get()
            if args is None:
                break
            msg = self.readdata(*args)
            result.append(msg)
            needmorequeue.put('msg=[%s], %d is done' % (msg, self.mynumber))
            self.dos3upload(*args)
            chunkqueue.task_done()


def split_offsets(in_file):
    size = os.path.getsize(in_file)
    return [ (i, min(chunk, size-i)) for i in range(0, size, chunk) ]

bucket_name = None
tarball = None

if len(sys.argv) < 2:
    print 'usage: fastupload.py file bucket'
    sys.exit(1)

tarball = sys.argv[1]

if len(sys.argv) < 3 or sys.argv[2] is None:
    bucket_name = 'defaultbucket'
else:
    bucket_name = sys.argv[2]

print 'bucket name', bucket_name

s3_key_name = None
if s3_key_name is None:
    s3_key_name = os.path.basename(tarball)

conn = boto.connect_s3(host=s3endpoint,
                       is_secure=True
                       )

#print 'available locations'
#print '\n'.join(i for i in dir(Location) if i[0].isupper())

bucket = conn.lookup(bucket_name)
if not bucket:
#    conn.create_bucket(bucket_name, location=Location.USWest2)
    conn.create_bucket(bucket_name, location=Location.DEFAULT)
    bucket = conn.lookup(bucket_name)

mb_size = os.path.getsize(tarball) / 1e6
print 'mb_size ',mb_size
print 'bytes',os.path.getsize(tarball)

mp = bucket.initiate_multipart_upload(s3_key_name, reduced_redundancy=False)
print ''


f = io.open(tarball, mode='rb', buffering=4*1024*1024)

for i in range(64):
    w = Worker(i, bucket_name, f, mp.key_name, mp.id)
    w.setDaemon(1)
    w.start()


start = datetime.datetime.now()

for (i, (begin, length)) in enumerate(split_offsets(tarball)):
    chunkqueue.put((i, datetime.datetime.now(), (begin, length)))
    needmorequeue.get()
    needmorequeue.task_done()

chunkqueue.join()
dataend = datetime.datetime.now()
print 'join  waiting', dataend
f.close()

print ''
print 'firing complete upload'
mp.complete_upload()
print 'complete upload done'
finalend = datetime.datetime.now()

print 'file portion MB/s', (os.path.getsize(tarball)/(1024.0*1024.0))/((dataend-start).seconds + (dataend-start).microseconds/1e6)
print 'final        MB/s', (os.path.getsize(tarball)/(1024.0*1024.0))/((finalend-start).seconds + (finalend-start).microseconds/1e6)
print 'total time',(finalend-start).seconds + (finalend-start).microseconds/1e6
print 'time wasted', sum(timewasted)
