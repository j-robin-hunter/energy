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
import queue


def millis():
    return int(round(time.time() * 1000))


def sleep(seconds):
    return time.sleep(seconds)


class AbstractDatabase(ABC, threading.Thread):
    @abstractmethod
    def __init__(self, config, database_queue):
        super(AbstractDatabase, self).__init__()
        threading.Thread.__init__(self)
        self.terminate = threading.Event()
        self.config = config
        self.queue = database_queue

    @abstractmethod
    def __del__(self):
        pass

    def run(self):
        # noinspection PyBroadException
        try:
            # All modules exist within a main loop that will only exit if the module is requested to terminate
            # or it generates an error that causes it to terminate.
            while not self.terminate.is_set():
                try:
                    data = self.queue.get(block=True, timeout=1)
                    if data.get('meter_reading', None):
                        self.write_meter_reading(data['meter_reading'])
                    elif data.get('meter_tariff', None):
                        self.write_meter_tariff(data['meter_tariff'])

                except queue.Empty:
                    pass

            logging.info('Database "{}" is terminating following signal'.format(self.getName()))
            self.__del__()

        except Exception as e:
            logging.error('Exception caught {}: {}'.format(type(e), str(e)))

    def write_meter_reading(self, meter_reading):
        logging.error(
            'Database implementation "{}" '
            'unexpectedly called the default write_meter_reading() method '
            '- this should be overridden'.format(self.config["type"]))
        raise NotImplementedError(
            'Unexpected invocation of abstract class method write_meter_reading '
            '- this should be overridden in concrete class')

    def write_meter_tariff(self, meter_tariff):
        logging.error(
            'Database implementation "{}" '
            'unexpectedly called the default write_meter_tariff() method '
            '- this should be overridden'.format(self.config["type"]))
        raise NotImplementedError(
            'Unexpected invocation of abstract class method write_meter_tariff '
            '- this should be overridden in concrete class')


def meter_reading(tariffs):
    reading = 'positive'
    if tariffs['source'] == 'grid' and tariffs['type'] == 'income':
        reading = 'negative'
    return reading


class AbstractModule(ABC, threading.Thread):
    @abstractmethod
    def __init__(self, module, database_queue, tariff):
        super(AbstractModule, self).__init__()
        threading.Thread.__init__(self)
        self.terminate = threading.Event()
        self.module = module
        self.queue = database_queue
        self.tariff = tariff
        self.lasttariff = dict()

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

            logging.info('Module "{}" is terminating following signal'.format(self.getName()))
            self.__del__()
        except Exception as e:
            logging.error('Exception caught {}: {}'.format(type(e), str(e)))

    def process_outputs(self):
        logging.error(
            'Module "{}" unexpectedly called the default process_outputs() method '
            '- this should be overridden'.format(self.getName()))
        raise NotImplementedError(
            'Unexpected invocation of abstract class method process_outputs '
            '- this should be overridden in concrete class')

    def write_meter_reading(self, **kwargs):
        self.queue.put(dict(meter_reading=kwargs))

        if self.tariff:
            self.write_tariff(**kwargs)

    def write_tariff(self, **kwargs):
        if self.lasttariff.get(kwargs['id'], None) is not None:
            delta = (kwargs['time'] - self.lasttariff[kwargs['id']]['read_at']) / 1000
            tariff = dict(
                time=self.lasttariff[kwargs['id']]['time'],
                id=kwargs['id'],
                name=self.lasttariff[kwargs['id']]['name'],
                amount=abs((kwargs['reading'] / 1000) * self.lasttariff[kwargs['id']]['rate'] * delta),
                tariff=self.lasttariff[kwargs['id']]['tariff'],
                tax=self.lasttariff[kwargs['id']]['tax'],
                rateid=self.lasttariff[kwargs['id']]['rateid'],
                type=self.lasttariff[kwargs['id']]['type'],
                source=self.lasttariff[kwargs['id']]['source']
            )
            if tariff['amount'] > 0:
                self.queue.put(dict(meter_tariff=tariff))

        for tariffs in [d for d in self.tariff
                        if d['id'] == kwargs['id'] and d['module'] == kwargs['module']]:
            reading = meter_reading(tariffs)

            # While the if...else here looks to be doing the same thing
            # in each part they are processing different ids and hence
            # different tariff entries
            if reading == 'negative' and kwargs['reading'] < 0:
                self.process_tariff(tariffs, **kwargs)
            if reading == 'positive' and kwargs['reading'] >= 0:
                self.process_tariff(tariffs, **kwargs)

    def process_tariff(self, tariffs, **kwargs):
        try:
            rate = 0
            i = 0
            for i in range(len(tariffs['rate']) - 1, -1, -1):
                tax = 1 + float(tariffs['rate'][i]['tax'].strip('%')) / 100.0
                rate = tariffs['rate'][i]['amount'] / 3600 * tax
                h, m, s = tariffs['rate'][i]['start'].split(':')
                rate_start = int(h) * 3600 + int(m) * 60 + int(s)
                if int((kwargs['time'] / 1000) % 86400) >= rate_start:
                    break

            self.lasttariff[kwargs['id']] = dict(time=kwargs['time'],
                                                 reading=kwargs['reading'],
                                                 read_at=kwargs['time'],
                                                 rate=rate,
                                                 tariff=str(tariffs['rate'][i]['amount'])
                                                 + '/'
                                                 + tariffs['rate'][i]['unit'],
                                                 tax=tariffs['rate'][i]['tax'],
                                                 type=tariffs['type'],
                                                 name=tariffs['name'],
                                                 rateid=tariffs['rate'][i].get('rateid', ''),
                                                 source=tariffs['source'])

        except Exception:
            raise


class AbstractDataLoader(ABC, DataLoader):

    @abstractmethod
    def __init__(self, database):
        super(AbstractDataLoader, self).__init__()
        self.database = database

    def batch_load_fn(self, keys):
        logging.error(
            'Dataloader "{}" unexpectedly called the dataload_fn() method '
            '- this should be overridden'.format(self.getName()))
        raise NotImplementedError(
            'Unexpected invocation of abstract class method dataload_fn '
            '- this should be overridden in concrete class')
