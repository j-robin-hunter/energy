#!/usr/bin/env python
""" A formatter to convert data received from a GoodWe plugin to InfluxDB line format

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

from abstract import AbstractFormatter
import logging


class Formatter(AbstractFormatter):

    def __init__(self,  myname, queuename):
        super().__init__(myname, queuename)
        pass

    def __del__(self):
        pass

    def do_formatting(self, data):
        lineformat = []
        try:
            lineformat.append(
                'inverter,sn=%s,model=%s '
                'Vpv1=%d,'
                'Ipv1=%d,'
                'PGrid=%d,'
                'ETotal=%d,'
                'EDay=%d,'
                'Vbattery1=%d,'
                'SOC1=%d,'
                'Ibattery1=%d,'
                'PVTotal=%d,'
                'LoadPower=%d,'
                'E_Load_Day=%d,'
                'E_Total_Load=%d,'
                'SOH1=%d,'
                'Pmeter=%d'
                ' %d' %
                (data['serial'].replace(' ', '\ '),
                 data['model'].replace(' ', '\ '),
                 data['Vpv1'],
                 data['Ipv1'],
                 data['PGrid'],
                 data['ETotal'],
                 data['EDay'],
                 data['Vbattery1'],
                 data['SOC1'],
                 data['Ibattery1'],
                 data['PVTotal'],
                 data['LoadPower'],
                 data['E_Load_Day'],
                 data['E_Total_Load'],
                 data['SOH1'],
                 data['Pmeter'],
                 data['timestamp'] * 1000000))  # Convert to nanoseconds
        except KeyError as e:
            logging.debug('KeyError "%s" in formatter "%s"' % (str(e), self.myname))
        return lineformat
