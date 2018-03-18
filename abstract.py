#!/usr/bin/env python
""" Provides abstract class definitions.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = "Robin Hunter"
__contact__ = "rhunter@crml.com"
__copyright__ = "Copyright 2018, Roma Technology Limited"
__license__ = "GPLv3"
__status__ = "Production"
__version__ = "1.0.0"

from abc import ABC, abstractmethod
import threading
from queue import Empty
import logging


class AbstractPlugin(ABC, threading.Thread):
    @abstractmethod
    def __init__(self, myname, queues, formatters, config):
        super(AbstractPlugin, self).__init__()
        threading.Thread.__init__(self)
        self.terminate = threading.Event()
        self.myname = myname
        self.queues = queues
        self.formatters = formatters
        self.config = config

    @abstractmethod
    def __del__(self):
        pass

    @abstractmethod
    def run(self):
        pass


class AbstractFormatter(ABC):
    @abstractmethod
    def __init__(self, myname, queuename):
        super(AbstractFormatter, self).__init__()
        self.myname = myname
        self.queuename = queuename

    @abstractmethod
    def __del__(self):
        pass

    def get_name(self):
        return self.myname

    def get_queuename(self):
        return self.queuename

    @abstractmethod
    def do_formatting(self, data):
        return data


class AbstractDatabase(AbstractPlugin):
    @abstractmethod
    def __init__(self, myname, queues, formatters, config):
        super(AbstractDatabase, self).__init__(myname, queues, formatters, config)
        self.connection = None
        self.QUEUE_TIMEOUT = 1

    @abstractmethod
    def data_callback(self, queuename, data):
        pass

    def run(self):
        try:
            # A database plugin may read data from multiple queues so a short blocking get on each queue is started
            # and joined for each queue. The blocking timeout is to allow for the plugin thread to receive and respond
            # to a terminate signal
            while not self.terminate.is_set():
                # Start process worker threads for each queue that this database plugin can received data from
                for queuename, queue in self.queues.items():
                    # Does the queue have a formatter
                    formatter = None
                    for formatter in self.formatters:
                        if formatter.get_queuename() == queuename:
                            logging.debug('Adding formatter "%s" to queue "%s"' % (formatter.get_name(), queuename))
                            break
                    thread = threading.Thread(
                        name=queuename,
                        target=self._process_queue,
                        args=(queuename, queue, formatter,))
                    logging.debug('Starting listen thread for queue "%s"' % queuename)
                    thread.start()
                    thread.join()

            logging.info('Plugin "%s" is terminating following signal' % self.myname)
        except Exception as e:
            logging.error('Exception caught %s: %s' % (type(e), e))

    def _process_queue(self, queuename, queue, formatter):
        logging.debug('Process queue thread for queue "%s"' % queuename)
        # This process queue function will block on the queue until it receives data. Once
        # it has data it will format it and write it to the database
        try:
            data = queue.get(True, self.QUEUE_TIMEOUT)
            if formatter is not None:
                if formatter.get_queuename() == queuename:
                    logging.debug('Passing data to formatter "%s"' % formatter.get_name())
                    data = formatter.do_formatting(data)
                    logging.debug('Calling "%s" with %s' % (self.myname, str(data)))
                    self.data_callback(queuename, data)
            logging.debug('Processing data from queue "%s"' % queuename)
        except Empty:
            pass
        except Exception:
            raise


class AbstractCollector(AbstractPlugin):
    @abstractmethod
    def __init__(self, myname, queues, formatters, config):
        super(AbstractCollector, self).__init__(myname, queues, formatters, config)

    @abstractmethod
    def run(self):
        pass

    def _data_send(self, data):
        for queuename, queue in self.queues.items():
            # Does the queue have a formatter
            for formatter in self.formatters:
                if formatter.get_queuename() == queuename:
                    logging.debug('Calling formatter "%s" prior to sending data on queue "%s"'
                                  % (formatter.get_name(), queuename))
                    data = formatter.do_formatting(data)
            logging.debug('Sending data on queue "%s"' % queuename)
            queue.put(data)
