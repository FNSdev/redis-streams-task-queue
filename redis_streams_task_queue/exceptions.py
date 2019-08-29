class BaseTaskQueueException(Exception):
    def __init__(self, message):
        super.__init__(message)
        self.message = message


class TaskDoesNotExistException(BaseTaskQueueException):
    pass


class QueueIsAlreadyRegisteredException(BaseTaskQueueException):
    pass
