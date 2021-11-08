#
# For live chain project
#
# @serco. Lavaux Gilles 2021-09-xx
#
import sys, traceback
import time
from datetime import datetime
import threading
from typing import Optional
from constants import *
from interfaces.iStatus import iStatus
import helpers.inbox.inboxUtils as inboxUtils
from watchdog.observers import Observer
from helpers.inbox.crawler.crawlerHandlerLandsat8 import CrawlerHandlerLandsat8
import configuration
from configuration import Configuration
# from watchdog.events import EVENT_TYPE_CREATED, EVENT_TYPE_MOVED, EVENT_TYPE_DELETED, EVENT_TYPE_MODIFIED, EVENT_TYPE_CLOSED
# from helpers.inbox.watcher.watchdogHandler import WatchdogHandler
from notification.redisPublisher import RedisPublisher
from workflow.queues.itemsQueue import ItemsQueue

import myLoggerConfig

VERSION = "LiveChain component: InboxCrawler V:0.1.0"
COMPONENT = "live-chain_InboxCrawler"


class InboxCrawler(threading.Thread, iStatus):

    def __init__(self, my_name: str,
                 inPath: str,
                 outPath: str,
                 failPath: str,
                 itemQueue: ItemsQueue,
                 config: Configuration,
                 aPattern: [str]):
        threading.Thread.__init__(self)
        self.myName = my_name
        self.logger = myLoggerConfig.applyLoggingLevel(f"{self.__class__.__name__}/{self.myName}", True,
                                                       f"/{self.name}")
        self.inPath = inPath
        self.outPath = outPath
        self.failPath = failPath
        self.itemQueue = itemQueue
        self.config = config
        self.pattern = aPattern
        self.logger.info(VERSION)
        self.running = False
        self.handler = None
        self.logger.info(
            f" @@@@@@@@@@@@@@@@@@@@@@@@@@ crawling folder: {self.inPath} with pattern: {self.pattern}, will move into: {self.outPath}")

    #
    #
    #
    def status(self, format: Optional[str] = FORMAT_TEXT):
        if self.handler is not None:
            return self.handler.status(format)
        else:
            return "no handler!"

    #
    #
    #
    def getQueue(self):
        return self.itemQueue

    #
    #
    #
    def run(self):
        pubblisher = RedisPublisher()
        current_time = str(datetime.now())
        self.running = True
        pubblisher.publish(COMPONENT, f"{COMPONENT} started at {current_time}")
        self.handler = CrawlerHandlerLandsat8(self.myName,
                                              self.itemQueue,
                                              pubblisher,
                                              inPath=self.inPath,
                                              # self.config.missionConfig['landsat8'][configuration.SETTING_INBOX],
                                              outPath=self.outPath,
                                              # self.config.missionConfig['landsat8'][configuration.SETTING_VALIDATED]
                                              failPath=self.failPath
                                              )
        try:
            while self.running:
                self.crawlFolder()
                time.sleep(10)
        except KeyboardInterrupt:
            self.running = False

    #
    #
    #
    def crawlFolder(self):
        aList = inboxUtils.getFoldersInDir(self.inPath, self.pattern, True)
        self.logger.debug(f"crawling folder {self.inPath}: found {len(aList)} inputs: {aList}")  # ; dir: {dir(aList)}")
        for item in aList:
            print(f"  --> {item}")
            self.handler.process(event=item)


if __name__ == "__main__":
    try:
        if len(sys.argv) < 3:
            print("syntax: python3 inboxAgent inboxPath pattern")
            sys.exit(1)

        current_time = str(datetime.now())
        aPath = sys.argv[1]
        aPattern = sys.argv[2]
        print(f"will watch folder {aPath} using pattern: {aPattern}\n")
        from helpers.inbox.inboxDummyHandler import InboxDummyHandler

        anHandler = InboxDummyHandler()
        agent = InboxCrawler('test_input_crawler', aPath, anHandler, sys.argv[2])
        agent.start()
        sys.exit(0)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(f"Error {exc_type} {exc_obj}")
        traceback.print_exc(file=sys.stdout)
        sys.exit(2)
