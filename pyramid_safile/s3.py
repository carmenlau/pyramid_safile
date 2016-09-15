import uuid
import tempfile
import logging
import mimetypes
import base64
import hashlib
import hmac
import time
from urllib import parse

import tinys3

from .error import FileHandleError
from .base import FileHandleBase, FileHandleFactoryBase

log = logging.getLogger(__name__)


class S3FileHandleFactory(FileHandleFactoryBase):
    
    def __init__(self, url, config):
        self.bucket = url.netloc
        self.access = config['s3.' + self.bucket + '.access']
        self.secret = config['s3.' + self.bucket + '.secret']
        self.conn = tinys3.Connection(self.access, self.secret,
            tls=True, default_bucket=self.bucket)

    def create_handle(self, original_filename, fp):
        return S3FileHandle(self, original_filename, fp)

    def from_descriptor(self, descriptor):
        return S3FileHandle.from_descriptor(self, descriptor)

    def upload(self, *args, **kwargs):
        self.conn.upload(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self.conn.delete(*args, **kwargs)

    def sign_obj(self, obj_key, expires=300):
        # TODO: steping expire for better browser cache
        filename = obj_key
        filename = filename.replace('%2F', '/')
        path = '/%s/%s?response-content-disposition=attachment;'\
            % (self.bucket, filename)

        expire_time = time.time() + expires

        expire_str = '%.0f' % (expire_time)
        string_to_sign = u'GET\n\n\n%s\n%s' % (expire_str, path)
        params = {
            'AWSAccessKeyId': self.access,
            'Expires': expire_str,
            'Signature': self.gen_signature(string_to_sign),
            'response-content-disposition': 'attachment;'
        }
        return 'http://%s.s3.amazonaws.com/%s?%s' % \
            (self.bucket, filename, parse.urlencode(params))

    def gen_signature(self, string_to_sign):
        return base64.encodestring(
            hmac.new(
                self.secret.encode('utf-8'),
                string_to_sign.encode('utf-8'),
                hashlib.sha1
            ).digest()
        ).strip()


class S3FileHandle(FileHandleBase):

    schema = 's3'

    def __init__(self, factory, original_filename, fp=None):
        self.factory = factory
        self.bucket = factory.bucket
        self.filename = original_filename
        if fp is not None:
            self.key = uuid.uuid4().hex
            log.debug('Uploading file to S3...')
            log.debug('Original filename: %s' % self.filename)
            log.debug('S3 bucket: %s' % self.bucket)
            log.debug('S3 key: %s' % self.key)
            # set Content-Type in header 
            content_type = mimetypes.guess_type(original_filename)[0]
            self.factory.upload(
                self.obj_key, fp,
                content_type=content_type, public=False)
            self._size = self.get_file_size(fp)
            log.debug('File uploaded.')

    @property
    def obj_key(self):
        '''
        >>> from mock import Mock
        >>> factory = Mock()
        >>> handle = S3FileHandle(factory, 'msg-30599-141.msg')
        >>> handle.key = 'uuid'
        >>> handle.obj_key
        'uuid/msg-30599-141.msg'
        >>> handle = S3FileHandle(factory, '技能训练.txt')
        >>> handle.key = 'uuid'
        >>> handle.obj_key
        'uuid/%E6%8A%80%E8%83%BD%E8%AE%AD%E7%BB%83.txt'
        '''
        return "%s/%s" % (self.key, parse.quote(self.filename))
        
    def delete(self):
        return self.factory.delete(self.obj_key)

    def tempfile(self):
        f = tempfile.TemporaryFile()
        r = self.factory.get(self.obj_key)
        f.seek(0)
        return f

    @property
    def url(self):
        return self.factory.sign_obj(self.obj_key)

    @property
    def size(self):
        return self._size

    @property
    def descriptor(self):
        return {
            'storage': 's3',
            'path': self.key,
            'filename': self.filename,
            'size': self.size
        }

    @classmethod
    def from_descriptor(cls, factory, descriptor):
        self = cls(factory, descriptor['filename'])
        self.key = descriptor['path']
        self._size = descriptor['size']
        return self
