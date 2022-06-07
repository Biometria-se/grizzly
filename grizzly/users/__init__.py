'''
@anchor pydoc:grizzly.users Load User
This package contains implementation for different type of endpoints and protocols.

These implementations are the basis for how to communicate with the system under test.

## Custom

It is possible to implement custom users, the only requirement is that they inherit `grizzly.users.base.GrizzlyUser`. To get them to be executed by `grizzly`
the full namespace has to be specified as `user_class_name` in the scenarios {@pylink grizzly.steps.scenario.user} step.

There are examples of this in the {@link framework.example}.
'''
import logging


logger: logging.Logger = logging.getLogger(__name__)


from .restapi import RestApiUser
from .messagequeue import MessageQueueUser
from .servicebus import ServiceBusUser
from .sftp import SftpUser
from .blobstorage import BlobStorageUser

__all__ = [
    'RestApiUser',
    'MessageQueueUser',
    'ServiceBusUser',
    'SftpUser',
    'BlobStorageUser',
]
