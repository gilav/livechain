import configparser
from . import semaphore

aGlobalSemaphore=semaphore.Semaphore()
aGlobalSemaphore.start()