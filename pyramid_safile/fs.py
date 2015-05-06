import os
import uuid
import logging
import mimetypes
import shutil
from urllib.parse import urlunparse

from .base import FileHandleBase, FileHandleFactoryBase


log = logging.getLogger(__name__)

class FileSystemHandleFactory(FileHandleFactoryBase):
    
    def __init__(self, url, config):
        self.path = url.path
        self.asset_path = config['fs.' + self.path + '.asset_path']

    def create_handle(self, filename, fp):
        return FileSystemHandle(self.path, self.asset_path, filename, fp=fp)

    def from_descriptor(self, descriptor):
        return FileSystemHandle.from_descriptor(
            self.path, self.asset_path, descriptor)


class FileSystemHandle(FileHandleBase):

    schema = 'fs'

    def __init__(self, path, asset_path, original_filename, fp=None):
        self.path = path
        self.asset_path = asset_path
        self.filename = original_filename
        if fp is not None:
            self.key = uuid.uuid4().hex
            log.debug('Copying file to file system...')
            log.debug('Original filename: %s' % self.filename)
            log.debug('Path: %s' % self.path)
            log.debug('key: %s' % self.key)
            os.makedirs(os.path.join(self.path, self.key))
            fdst = open(self.dst, 'wb+')
            shutil.copyfileobj(fp, fdst)
            self._size = self.get_file_size(fp)
            log.debug('File uploaded.')

    def delete(self):
        os.remove(self.dst)
        os.rmdir(os.path.join(self.path, self.key))

    def tempfile(self):
        return open(self.dst, 'r')

    @property
    def dst(self):
        return os.path.join(self.path, self.key, self.filename)

    @property
    def url(self):
        return self.asset_path + "%s/%s" % (self.key, self.filename)

    @property
    def size(self):
        return self._size

    @property
    def descriptor(self):
        return {
            'storage': 'fs',
            'path': self.key,
            'filename': self.filename,
            'size': self.size
        }

    @classmethod
    def from_descriptor(cls, path, asset_path, descriptor):
        self = cls(path, asset_path, descriptor['filename'])
        self.key = descriptor['path']
        self._size = descriptor['size']
        return self
