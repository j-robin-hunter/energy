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
            'Database implementation "{}" '
            'unexpectedly called the default write_meter_reading() method '
            '- this should be overridden'.format(self.config["type"]))
        raise NotImplementedError(
            'Unexpected invocation of abstract class method write_meter_reading '
            '- this should be overridden in concrete class')

    def all_latest_meter_readings(self):
        logging.error(
            'Database implementation "{}" '
            'unexpectedly called the default all_latest_measurements() method '
            '- this should be overridden'.format(self.config["type"]))
        raise NotImplementedError(
            'Unexpected invocation of abstract class method all_latest_measurements '
            '- this should be overridden in concrete class')

    def all_meter_readings_between(self, start, end=None):
        logging.error(
            'Database implementation "{}" '
            'unexpectedly called the default all_measurements_between() method '
            '- this should be overridden'.format(self.config["type"]))
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

            logging.info('Plugin "{}" is terminating following signal'.format(self.getName()))
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

        self.write_tariff(**kwargs)

    def write_tariff(self, **kwargs):
        if self.lasttariff.get(kwargs['id'], None) is not None:
            delta = (kwargs['time'] - self.lasttariff[kwargs['id']]['read_at']) / 1000
            tariff = dict(
                time=self.lasttariff[kwargs['id']]['time'],
                id=kwargs['id'],
                amount=abs((kwargs['reading'] / 1000) * self.lasttariff[kwargs['id']]['rate'] * delta),
                tariff=self.lasttariff[kwargs['id']]['tariff'],
                tax=self.lasttariff[kwargs['id']]['tax'],
                type=self.lasttariff[kwargs['id']]['type'],
                name=self.lasttariff[kwargs['id']]['name'],
                rateid=self.lasttariff[kwargs['id']]['rateid']
            )
            if tariff['amount'] > 0:
                result = self.schema.execute(
                    '''
                    mutation CreateMeterTariff($tariff: MeterTariffInput!) {
                        createMeterTariff(meterTariff: $tariff) {
                            time
                        }
                    }
                    ''',
                    variable_values={
                        "tariff": tariff
                    },
                    context_value={"database": self.database}
                )
                if result.errors:
                    logging.error(result.errors)
                    raise RuntimeError("Error in GraphQL mutation")

        for tariffs in [d for d in self.tariff
                        if d['meter']['id'] == kwargs['id'] and d['meter']['source'] == kwargs['source']]:
            meter_values = tariffs['meter'].get('meter_values', 'both')

            # While the if...else here looks to be doing the same thing
            # in each part they are processing different ids and hence
            # different tariff entries
            if meter_values == 'negative' and kwargs['reading'] < 0:
                self.process_tariff(tariffs, **kwargs)
            elif meter_values == 'positive' and kwargs['reading'] >= 0:
                self.process_tariff(tariffs, **kwargs)
            elif meter_values == 'both':
                self.process_tariff(tariffs, **kwargs)

    def process_tariff(self, tariffs, **kwargs):
        try:
            rate = 0
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
                                                 rateid=tariffs['rate'][i].get('rateid', ''))
        except Exception:
            raise

    def write_meter_tariff(self, **kwargs):
        result = self.schema.execute(
            '''
            mutation CreateMeterTariff($reading: MeterTariffInput!) {
                createMeterTariff(meterTariff: $tariff) {
                    time
                }
            }
            ''',
            variable_values={
                "tariff": kwargs
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
            'Dataloader "{}" unexpectedly called the dataload_fn() method '
            '- this should be overridden'.format(self.getName()))
        raise NotImplementedError(
            'Unexpected invocation of abstract class method dataload_fn '
            '- this should be overridden in concrete class')
