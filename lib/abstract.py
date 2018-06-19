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
import logging
import time
from promise.dataloader import DataLoader


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

    def write_meter_reading(self, meter_reading):
        logging.error(
            f'Database implementation "{self.config["type"]}" '
            f'unexpectedly called the default write_meter_reading() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method write_meter_reading '
            '- this should be overridden in concrete class')

    def all_latest_meter_readings(self):
        logging.error(
            f'Database implementation "{self.config["type"]}" '
            f'unexpectedly called the default all_latest_measurements() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method all_latest_measurements '
            '- this should be overridden in concrete class')

    def all_meter_readings_between(self, start, end=None):
        logging.error(
            f'Database implementation "{self.config["type"]}" '
            f'unexpectedly called the default all_measurements_between() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method all_measurements_between '
            '- this should be overridden in concrete class')


class AbstractModule(ABC, threading.Thread):
    @abstractmethod
    def __init__(self, module, schema, database, tariff):
        super(AbstractModule, self).__init__()
        threading.Thread.__init__(self)
        self.terminate = threading.Event()
        self.module = module
        self.schema = schema
        self.database = database
        self.tariff = tariff
        self.QUEUE_TIMEOUT = 0.1

    @abstractmethod
    def __del__(self):
        pass

    def run(self):
        # noinspection PyBroadException
        try:
            # All modules exist within a main loop that will only exit if the module is requested to terminate
            # or it generates an error that causes it to terminate.
            while not self.terminate.is_set():
                self.process_outputs()

            logging.info(f'Plugin "{self.getName()}" is terminating following signal')
        except Exception as e:
            logging.error(f'Exception caught {type(e)}: {str(e)}')

    def process_outputs(self):
        logging.error(
            f'Module "{self.getName()}" unexpectedly called the default process_outputs() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method process_outputs '
            '- this should be overridden in concrete class')

    def write_meter_reading(self, **kwargs):
        result = self.schema.execute(
            '''
            mutation CreateMeterReading($reading: MeterReadingInput!) {
                createMeterReading(meterReading: $reading) {
                    time
                }
            }
            ''',
            variable_values={
                "reading": kwargs
            },
            context_value={"database": self.database}
        )
        if result.errors:
            logging.error(result.errors)
            raise RuntimeError("Error in GraphQL mutation")


class AbstractDataLoader(ABC, DataLoader):

    @abstractmethod
    def __init__(self, database):
        super(AbstractDataLoader, self).__init__()
        self.database = database

    def batch_load_fn(self, keys):
        logging.error(
            f'Dataloader "{self.getName()}" unexpectedly called the dataload_fn() method '
            f'- this should be overridden')
        raise NotImplementedError(
            'Unexpected invocation of abstract class method dataload_fn '
            '- this should be overridden in concrete class')
