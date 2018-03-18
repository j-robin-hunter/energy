#!/usr/bin/env python
""" A plugin to provide a collector for electricity meter data using the Enisitic Smart Meter suite

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

from abstract import AbstractCollector
import socket
import re
import logging
import time


def millis():
    return int(round(time.time() * 1000))


def get_number(x):
    return int(''.join(ele for ele in x if ele.isdigit()))


class Plugin(AbstractCollector):
    def __init__(self, myname, queues, formatters, config):
        super().__init__(myname, queues, formatters, config)
        try:
            self.modelName = None
            self.serialNumber = None

            self.meter_port = self.config['port']
            self.meters = self.config['meters']

            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(('0.0.0.0', self.meter_port))
        except KeyError as e:
            logging.error('Invalid key %s in %s configuration' % (str(e), self.myname))
            raise RuntimeError('Error in %s plugin configuration' % self.myname)

    def __del__(self):
        try:
            if self.sock is not None:
                self.sock.close()
        except Exception as e:
            logging.error('Encountered error while disposing %s: %s' % (self.myname, str(e)))

    def run(self):
        try:
            while not self.terminate.is_set():
                data, addr = self.sock.recvfrom(4096)
                if data.find(b"D1") > 0 and self.serialNumber is not None and self.modelName is not None:
                    logging.debug('Enistic meter reading "%s"', data)
                    index = data.find(b"D1")
                    tokens = data[index:len(data)].decode('utf-8').split(',')
                    if len(tokens) >= 6:
                        meter_reading = {'timestamp': millis(), 'serial': self.serialNumber,
                                         'model': self.modelName, 'power': get_number(tokens[3])}
                        channel = list(filter(lambda meter: meter['channel'] == get_number(tokens[5]), self.meters))
                        try:
                            meter_reading['name'] = channel[0].get('name')
                            meter_reading['category'] = channel[0].get('category')

                        # InitialPhase the event that the channel has not been defined in the config file
                        # there will be an IndexError. If this occurs then set a default name and category
                        except IndexError:
                            meter_reading['name'] = 'Channel ' + str(get_number(tokens[5]))
                            meter_reading['category'] = 'General'

                        logging.debug('Writing Enisitic data to measurement queue')
                        self._data_send(meter_reading)
                else:
                    try:
                        self.serialNumber = re.search('Core:Serial=(.+?)\r', data.decode('utf-8')).group(1)
                        logging.debug('Enistic serial number record "%s"', data)
                    except AttributeError:
                        try:
                            self.modelName = re.search('Status:Model=(.+?)\r', data.decode('utf-8')).group(1)
                            logging.debug('Enistic model nasme record "%s"', data)
                        except AttributeError:
                            pass

            logging.info('Plugin "%s" is terminating following signal' % self.myname)

        except Exception:
            raise
