'''Our internal exceptions'''


class S3POException(Exception):
    '''All of S3PO's exceptions'''
    pass


class DownloadException(S3POException):
    '''An error while downloading'''
    pass


class UploadException(S3POException):
    '''An error while uploading'''
    pass


class DeleteException(S3POException):
    '''An error while deleting'''
    pass
