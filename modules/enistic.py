#!/usr/bin/env python
""" A plugin to provide a database connection using the InfluxDB time series database

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

from lib.abstract import AbstractModule
import socket
import re
import logging
from datetime import datetime
import time


def get_number(x):
    return int(''.join(ele for ele in x if ele.isdigit()))


class Module(AbstractModule):
    def __init__(self, module, schema, database, tariff):
        super().__init__(module, schema, database, tariff)
        try:
            self.model_name = None
            self.serial_number = None
            self.enistic_time_offset = None
            self.last_reading_time = 0

            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('0.0.0.0', self.module['port']))
        except KeyError as e:
            logging.error('Invalid key %s in %s configuration' % (str(e), self.getName()))
            raise RuntimeError('Error in %s plugin configuration' % self.getName())

    def __del__(self):
        try:
            if self.sock is not None:
                self.sock.close()
        except Exception as e:
            logging.error('Encountered error while disposing %s: %s' % (self.getName(), str(e)))

    def process_outputs(self):
        try:
            data, addr = self.sock.recvfrom(4096)
            index = data.find(b"D1")
            if index > 0 and self.enistic_time_offset is not None:
                logging.debug('Enistic meter reading "%s"', data)
                reading_time = \
                    self.enistic_time_to_now(re.search('\d*\/\d*\/\d*\/\d*\/\d*\/\d*', data.decode('utf-8')).group(0))\
                    + self.enistic_time_offset
                tokens = data[index:len(data)].decode('utf-8').split(',')
                if len(tokens) >= 6:
                    try:
                        meter_reading = get_number(tokens[3]) / 1000
                        meter = list(
                                filter(lambda meter: meter['channel'] == get_number(tokens[5]), self.module['meter'])
                            )[0]
                        if reading_time != self.last_reading_time:
                            self.write_meter_reading(
                                time=reading_time * 1000,
                                source=self.module['name'],
                                id=meter['id'],
                                reading=meter_reading,
                                unit='watts')
                            self.last_reading_time = reading_time

                    # Ignore index as it is a meter that has not been defined as having a
                    # channel/name lookup
                    except IndexError:
                        pass
            else:
                try:
                    self.enistic_time_offset = round(time.time()) - self.enistic_time_to_now(
                                         re.search('Status:TimeNow=(.+?)\r', data.decode('utf-8')).group(1))
                except AttributeError:
                    try:
                        if self.serial_number is None:
                            self.serial_number = re.search('Core:Serial=(.+?)\r', data.decode('utf-8')).group(1)
                            logging.info('Serial number for module "{}"'
                                         ' is "{}"'.format(self.module["name"], self.serial_number))
                    except AttributeError:
                        try:
                            if self.model_name is None:
                                self.model_name = re.search('Status:Model=(.+?)\r', data.decode('utf-8')).group(1)
                                logging.info('Model name for module "{}"'
                                             ' is "{}"'.format(self.module["name"], self.model_name))
                        except AttributeError:
                            pass
        except Exception as e:
            logging.error(str(e))
            raise RuntimeError(str(e))

    def enistic_time_to_now(self, enistic_time):
        # The clock on the enistic meter is not capable of being set and it does not
        # keep very good time. It does however report it's time (reset to 1st Jan 2000 on power cycle)
        # every minute or so and this can be used to adjust each data reading to a close approximation
        # to 'real time' (+- 3 seconds or so). There is a 'risk' that a power cycle of the enistic meter
        # will result in a small number or readings (less than 6 or so) going backwards by a considerable
        # period. The impact of this happening is minimal so this risk is not coded for.
        t = enistic_time.split('/')
        return round(datetime(int(t[2]), int(t[1]), int(t[0]), int(t[3]), int(t[4]), int(t[5])).timestamp())
