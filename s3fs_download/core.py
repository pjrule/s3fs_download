import io
import os
import tempfile
from s3fs import S3FileSystem, S3File
from s3fs.core import split_path
from botocore.exceptions import ClientError

DEFAULT_BUFFER_SIZE = 2**20 * 256

class S3Downloader(S3FileSystem):
    def __init__(self, dir='', lazy=False, use_cache=False, *args, **kwargs):
        """
        Initialize an S3Downloader, a derivative of the s3fs.S3FileSystem class.
        Parameters:
        - dir: directory to save downloaded files in. If the directory does not exist, it will be created. Downloaded files will be stored at <dir>/<key>.
        - lazy: Lazy-load files--download on read, not on open.
        - use_cached: Use cached files if they exist at <dir>/<key>. Useful if you're processing the same data multiple times. Does not check to see if files is stale relative to the S3 bucket.
        """
        self.dir = dir
        self.lazy = lazy
        self.use_cache = use_cache
        
        super(S3Downloader, self).__init__(*args, **kwargs)
    
    def open(self, path):
        return DownloadedS3File(self, path)

class DownloadedS3File(S3File):
    """
    DownloadedS3File is a subclass of s3fs.S3File that only supports reading files.
    Parameters:
    - s3: an S3Downloader class.
    - path: <bucket>/<key> path of the file on S3.
    - mode: file read mode. Currently, the only choice is 'rb'.
    - buf_size: size of chunk to read from S3 before writing to disk. Default: 256MB. Decrease if you're running out of RAM.
    """
    def __init__(self, s3, path, mode='rb', buf_size = DEFAULT_BUFFER_SIZE, s3_additional_kwargs=None):
       # The goal here is to wrap the S3File API as much as possible. 
       # Including the mode parameter may be useful for backward compatibility--even though it's currently meaningless, some people's code may specify mode='rb' explicitly.
       # Thus, the API would break if the mode parameter was not included.
       # TODO: implement low-level multipart write?
        if mode != 'rb':
           raise NotImplementedError("File mode must be 'rb'")
           
        self.s3 = s3
        self.mode = mode
        self.path = path
        self.bucket, self.key = split_path(path)
        self.s3_additional_kwargs = s3_additional_kwargs or {}
        self.buf_size = DEFAULT_BUFFER_SIZE
        self.closed = False
        self._file = None
        self._tmp = None
        self._downloaded = False
        
        if not self.s3.lazy:
            self._file = self._get_file()         
            self._downloaded = True

    def read(self, length=-1, force_refresh=False):
        """
        Read downloaded file from cache. Will download if lazy loading is enabled.
        """ 
        # TODO blazingly fast reads: http://rabexc.org/posts/io-performance-in-python
        if not self._file:
            self._file = self._get_file()
        if self._file.closed:
            raise IOError("Cache closed")

        with self._file as f:
            if length == -1:
                return f.read()
            else:
                return f.read(length)

    def readline(self):
        """
        Read a line of the downloaded file from cache. Will download if lazy loading is enabled.
        """
        if not self._file:
            self._file = self._get_file()
        if self._file.closed:
            raise IOError("Cache closed")

        return self._file.readline()

    def readlines(self):
        """
        Read all lines of the downloaded file from cache. Will download if lazy loading is enabled.
        """
        if not self._file:
            self._file = self._get_file
        if self._file.closed:
            raise IOError("Cache closed")

        return self._file.readlines()
    
    def write(self, *args, **kwargs):
        """
        Not implemented.
        """
        _read_only()
    
    def flush(self, *args, **kwargs):
        """
        Not implemented.
        """
        _read_only()

    def close(self):
        """
        Close cache file.
        """
        if self._file:
            self._file.close()
        self.closed = True
        
    def _get_tmp(self):
        if self.s3.dir:
            dir = os.path.join(self.s3.dir, os.sep.join(self.key.split(os.sep)[0:-1]))
            if not os.path.isdir(dir):
                os.makedirs(dir, exist_ok=True)
            return open(os.path.join(self.s3.dir, self.key), mode='wb+')
        else:
            return tempfile.TemporaryFile(mode='wb+')

    def _download(self):
        try:
            self._tmp = self._get_tmp()
            obj = self.s3.s3.get_object(Bucket=self.bucket, Key=self.key)['Body']
            data = obj.read(amt=self.buf_size)
            while data:
                self._tmp.write(data)
                data = obj.read(amt=self.buf_size)
        except ClientError:
            raise IOError("Couldn't download file. Verify that your S3 path is valid and that you have appropriate permissions.")
            
    def _get_file(self, force_refresh=False):
        if self.s3.use_cache and os.path.isfile(os.path.join(self.s3.dir, self.key)) and not force_refresh:            
            return open(os.path.join(self.s3.dir, self.key), mode='rb')

        else:
            if not self._downloaded:
                self._download()
                self._downloaded = True
            self._tmp.seek(0)
            return self._tmp

    def _read_only():
        raise NotImplementedError('DownloadedS3File is read-only. Use s3fs.S3File for writes.')
