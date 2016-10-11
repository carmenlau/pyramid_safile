class FileHandleFactoryBase(object):
    def __init__(self, url, config):
        raise NotImplementedError

    def create_handle(self, original_filename, fp, **kwargs):
        raise NotImplementedError

    def from_descriptor(self, descriptor):
        raise NotImplementedError


class FileHandleBase(object):
    schema = 'base'
    def get_file_size(self, fp):
        orig_pos = fp.tell()
        fp.seek(0, 2)  # Go to end of file
        size = fp.tell()
        fp.seek(orig_pos)
        return size

    def delete(self):
        pass

    def tempfile(self):
        return None

    @property
    def url(self):
        return ''

    @property
    def size(self):
        return 0

    def descriptor(self):
        raise NotImplementedError

    @classmethod
    def from_descriptor(cls, descriptor):
        raise NotImplementedError
