import os
import uuid
import tempfile
import logging
import mimetypes
import base64
import hashlib
import hmac
import time
from urllib import parse
from urllib import request

import tinys3

from .error import FileHandleError
from .base import FileHandleBase, FileHandleFactoryBase

log = logging.getLogger(__name__)


class S3FileHandleFactory(FileHandleFactoryBase):

    def __init__(self, url, config):
        self.bucket = url.netloc
        self.access = config['s3.' + self.bucket + '.access']
        self.secret = config['s3.' + self.bucket + '.secret']
        self.endpoint = config.get(
            's3.' + self.bucket + '.endpoint', 's3.amazonaws.com')
        self.conn = tinys3.Connection(self.access, self.secret,
            tls=True, default_bucket=self.bucket, endpoint=self.endpoint)

    def create_handle(self, original_filename, fp, **kwargs):
        return S3FileHandle(self, original_filename, fp, **kwargs)

    def from_descriptor(self, descriptor):
        return S3FileHandle.from_descriptor(self, descriptor)

    def upload(self, *args, **kwargs):
        self.conn.upload(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self.conn.delete(*args, **kwargs)

    def public_url(self, obj_key):
        filename = obj_key
        filename = filename.replace('%2F', '/')
        return 'https://%s.s3.amazonaws.com/%s' % (self.bucket, filename)

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
        return 'https://%s.s3.amazonaws.com/%s?%s' % \
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

    def __init__(self, factory, original_filename, fp=None, **kwargs):
        self.factory = factory
        self.bucket = factory.bucket
        self.filename = original_filename
        self.public = bool(kwargs.get('public', False))
        if fp is not None:
            self.key = uuid.uuid4().hex
            if 'folder' in kwargs:
                self.key = '%s/%s' % (kwargs['folder'], self.key)
            log.debug('Uploading file to S3...')
            log.debug('Original filename: %s' % self.filename)
            log.debug('S3 bucket: %s' % self.bucket)
            log.debug('S3 key: %s' % self.key)
            # set Content-Type in header
            content_type = mimetypes.guess_type(original_filename)[0]
            self.factory.upload(
                self.obj_key, fp,
                content_type=content_type, public=self.public)
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
        _, file_extension = os.path.splitext(self.filename)
        f = tempfile.NamedTemporaryFile(suffix=file_extension)
        u = request.urlopen(self.url)
        meta = u.info()
        file_size = int(meta.get_all("Content-Length")[0])
        log.debug("Downloading to: %s (%s)" % (f.name, file_size))
        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break
            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%10d [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = status + chr(8)*(len(status)+1)
            log.debug(status)
        f.seek(0)
        return f

    @property
    def url(self):
        return self.factory.public_url(self.obj_key)\
                if self.public else self.factory.sign_obj(self.obj_key)

    @property
    def size(self):
        return self._size

    @property
    def descriptor(self):
        return {
            'storage': 's3',
            'path': self.key,
            'filename': self.filename,
            'size': self.size,
            'public': self.public,
        }

    @classmethod
    def from_descriptor(cls, factory, descriptor):
        self = cls(factory, descriptor['filename'])
        self.key = descriptor['path']
        self._size = descriptor['size']
        if 'public' in descriptor:
            self.public = descriptor['public']
        return self
