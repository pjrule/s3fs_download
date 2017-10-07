import os
import shutil
import s3fs
import pytest
import s3fs_download as s3dl
from random import randint
from time import time

@pytest.fixture(scope="module")
def bucket():
    bucket_name = "s3_download_test_" + str(time()) + "_" + str(randint(0, 10000000000))
    test_file = bucket_name + '/' + 'test_file'
    test_str = b'hello world!\n'
    fs = s3fs.S3FileSystem()
    fs.mkdir(bucket_name)
    with fs.open(test_file, 'wb') as f:
        f.write(test_str)
    yield {'bucket_name': bucket_name, 'test_file': test_file, 'test_str': test_str}
    fs.rm(test_file)
    fs.rmdir(bucket_name)

"""
Perform a basic read operation. Store the read file in the OS's temporary directory.
"""
def test_basic_read(bucket):
  d = s3dl.S3Downloader()
  with d.open(bucket['test_file']) as f:
    data = f.read()
    assert data == bucket['test_str']

"""
Download a file, save it to a specified directory for future use, and read it.
"""
def test_file_save_and_read(bucket):
    d = s3dl.S3Downloader(dir='tmp')
    with d.open(bucket['test_file']) as f:
      data = f.read()
      assert data == bucket['test_str']
    assert open(os.path.join('tmp', 'test_file'), 'rb').read() == bucket['test_str']
    shutil.rmtree('tmp')

"""
Show that caches are re-read from S3 on file open unless otherwise specified.
"""
def test_read_from_fresh_cache(bucket):
    d = s3dl.S3Downloader(dir='tmp')
    with d.open(bucket['test_file']) as f:
      data = f.read()
      assert data == bucket['test_str']
    with open(os.path.join('tmp', 'test_file'), 'w') as f:
      # Overwrite the cache
      f.write('bla')
    with d.open(bucket['test_file']) as f:
      data = f.read()
      assert data == bucket['test_str']
    shutil.rmtree('tmp')

"""
Show that caches can be persisted on disk across file opens and *not* re-read from S3, if specified, at the 
expense of the possibility of a stale cache.
"""
def test_read_from_stale_cache(bucket):
    d = s3dl.S3Downloader(dir='tmp', use_cache=True)
    with d.open(bucket['test_file']) as f:
      data = f.read()
      assert data == bucket['test_str']
    with open(os.path.join('tmp', 'test_file'), 'w') as f:
      # Overwrite the cache
      f.write('bla')
    with d.open(bucket['test_file']) as f:
      data = f.read()
      assert data == b'bla'
    shutil.rmtree('tmp')
