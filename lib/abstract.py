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
import time


def millis():
    return int(round(time.time() * 1000))


def sleep(seconds):
    return time.sleep(seconds)


class AbstractDatabase(ABC):
    @abstractmethod
    def __init__(self, config):
        super(AbstractDatabase, self).__init__()
        self.config = config

    @abstractmethod
    def __del__(self):
        pass

    def write_measurement(self, measurement):
        logging.error(
            f'Database implementation "{self.config["type"]}" unexpectedly called the default write_data() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method write_data '
            '- this should be overridden in concrete class')


class AbstractModule(ABC, threading.Thread):
    @abstractmethod
    def __init__(self, module):
        super(AbstractModule, self).__init__()
        threading.Thread.__init__(self)
        self.terminate = threading.Event()
        self.module = module
        self.QUEUE_TIMEOUT = 0.1

    @abstractmethod
    def __del__(self):
        pass

    def get_config_value(self, key):
        try:
            if self.module.get('config', None) is None:
                return None
            return self.module['config'][key]
        except KeyError:
            raise

    def run(self):
        try:
            # If this module is able to input data it will do that from a queue that has been passed
            # as part of the module configuration data passed to the module at startup. This queue will
            # be read with a short blocking to ensure that the module main loop is regularly entered
            # and so that data can be output if necessary and so that a terminate signal can be actioned
            # if it has been received. More than one thread can be started against a queue to allow for
            # potentially improved database write performance. The process_inputs_callback function must
            # be thread safe
            for measurement in self.module.get('inputs', []):
                for i in range(self.module.get('input_processes', 1)):
                    logging.debug(f'Module "{self.getName()}" thread {i + 1} listening on "{measurement}" input queue')
                    queue = self.module['queues'][measurement]
                    thread = threading.Thread(
                        name=self.getName() + '_queue',
                        target=self._process_queue,
                        args=(queue,))
                    thread.start()

            # All modules exist within a main loop that will only exit if the module is requested to terminate
            # or it generates an error that causes it to terminate.
            while not self.terminate.is_set():
                # If this module has any outputs defined then call process_outputs() which will
                # need to return to this main loop so that a other tasks or a request
                # to terminate can be actioned. This method needs to be overridden in the concrete
                # class or a NotImplemented error will be raised.
                if self.module.get('outputs', None) is not None:
                    self.process_outputs()

            logging.info(f'Plugin "{self.getName()}" is terminating following signal')
        except Exception as e:
            logging.error(f'Exception caught {type(e)}: {str(e)}')

    def _process_queue(self, queue):
        # This process queue function will block for on the queue for QUEUE_TIMEOUT time or
        # until it receives data. Once it has data it will call the module callback function for the
        # data to be processed by the module as required
        # noinspection PyBroadException
        try:
            while True:
                # data = queue.get(True, self.QUEUE_TIMEOUT)
                data = queue.get(True)
                if data:
                    logging.debug(f'Received data from queue "{self.getName()}", calling process_inputs_callback')
                    self.process_inputs_callback(data)
        except Empty:
            pass
        except Exception as e:
            logging.critical(str(e))
            self.terminate.set()

    def process_inputs_callback(self, data):
        logging.error(
            f'Module "{self.getName()}" unexpectedly called the default process_input_callback() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method process_input_callback '
            '- this should be overridden in concrete class')

    def process_outputs(self):
        logging.error(
            f'Module "{self.getName()}" unexpectedly called the default process_outputs() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method process_outputs '
            '- this should be overridden in concrete class')

    def send_output_data(self, sensor, **kwargs):
        try:
            # Add the timestamp, the name of the sensor, the measurement type and
            # the units as these are part of all measurement points as defined by
            # the schema Point object which is the superclass of all measurement types
            if kwargs.get('timestamp', None) is None:
                kwargs['timestamp'] = millis()
            kwargs['sensor'] = sensor

            # Locate the output details for the sensor and if found
            for sensor_output in self.module['outputs']:
                if sensor == sensor_output['name']:
                    kwargs['category'] = sensor_output['category']
                    kwargs['unit'] = sensor_output['unit']

                    logging.debug(f'Writing to "{sensor_output["measurement"]}" queue data {str(kwargs)}')
                    queue = self.module['queues'][sensor_output['measurement']]
                    queue.put(kwargs)
                    break

        except Exception:
            raise
