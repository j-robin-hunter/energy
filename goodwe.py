#!/usr/bin/env python
""" A plugin to provide a collector for solar related data using GoodWe inverters

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
import time
import socket
import ipaddress
import struct
import logging


def millis():
    return int(round(time.time() * 1000))


def calculate_crc(buffer):
    crc = 0
    for i in range(len(buffer)):
        crc += buffer[i]
    return crc


def append_crc(buffer):
    buffer.extend(calculate_crc(buffer).to_bytes(2, byteorder='big'))


class NoInverterError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class MalFormedError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Plugin(AbstractCollector):
    # GoodWe control and function codes
    CC_REG = 0x00
    CC_READ = 0x01
    FC_QRYOFF = 0x00
    FC_RESOFF = 0x80
    FC_QRYID = 0x02
    FC_RESID = 0x82
    FC_QRYRUN = 0x01
    FC_RESRUN = 0x81
    FC_RESIGN = 0x86

    # Goodwe binary packet constants
    HEADER = bytearray([0xaa, 0x55])
    PACKET_OVERHEAD = 9

    # GoodWe binary packet types
    CHAR = 0
    SHORT = 1
    LONG = 2

    # GoodWe single phase run response packet data items decode dictionary. It contains the
    # name of the packet, it's size and a 'divisor' needed to return the data to the precision
    # units specified in the GoodWe protocol.
    single_phase = {
        "Vpv1": [SHORT, 10],
        "Vpv2": [SHORT, 10],
        "Ipv1": [SHORT, 10],
        "Ipv2": [SHORT, 10],
        "Vac1": [SHORT, 10],
        "Iac1": [SHORT, 10],
        "Fac1": [SHORT, 100],
        "PGrid": [SHORT, 1],
        "WorkMode": [SHORT, 1],
        "Temperature": [SHORT, 10],
        "ErrorMessage": [LONG, 1],
        "ETotal": [LONG, 10],
        "HTotal": [LONG, 1],
        "SoftVersion": [SHORT, 1],
        "WarningCode": [SHORT, 1],
        "PV2FaultValue": [SHORT, 10],
        "FunctionsBitValue": [SHORT, 1],
        "BUSVoltage": [SHORT, 10],
        "GFCICheckValue_SafetyCountry": [SHORT, 1],
        "EDay": [SHORT, 10],
        "Vbattery1": [SHORT, 10],
        "Errorcode": [SHORT, 1],
        "SOC1": [SHORT, 1],
        "Ibattery1": [SHORT, 10],
        "PVTotal": [SHORT, 10],
        "LoadPower": [LONG, 1],
        "E_Load_Day": [SHORT, 10],
        "E_Total_Load": [LONG, 10],
        "InverterPower": [SHORT, 1],
        "Vload": [SHORT, 10],
        "Iload": [SHORT, 10],
        "OperationMode": [SHORT, 1],
        "BMS_Alarm": [SHORT, 1],
        "BMS_Warning": [SHORT, 1],
        "SOH1": [SHORT, 1],
        "BMS_Temperature": [SHORT, 10],
        "BMS_Charge_I_Max": [SHORT, 1],
        "BMS_Discharge_I_Max": [SHORT, 1],
        "Battery_Work_Mode": [SHORT, 1],
        "Pmeter": [SHORT, 1]
    }

    # Program constants
    BUFFERSIZE = 1024
    OFFLINE_TIMEOUT = 30000

    OFFLINE = 1
    RUNNING = 2

    SOCKET_RETRIES = 100
    SOCKET_RETRIES_DELAY = 10

    def __init__(self, myname, queues, formatters, config):
        super().__init__(myname, queues, formatters, config)
        self.state = self.OFFLINE
        self.statetime = millis()
        self.lastReceived = millis()

        self.ap_address = 0x7f
        self.inverter_address = 0xB0

        self.idinfo = {}
        self.reading = {}

        # Set socket up to allow UDP with broadcast
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Set socket timeout to maximum expected data response delay from inverter as per protocol specifications
        self.sock.settimeout(0.5)

        # Using broadcast will allow setting of either a single address or a bit masked address
        # for the inverter in the configuration
        self.addr = str(ipaddress.IPv4Network(self.config['host'], strict=False).broadcast_address), self.config['port']

    def __del__(self):
        try:
            if self.sock is not None:
                self.sock.close()
        except Exception as e:
            logging.error('Encountered error while disposing %s: %s' % (self.myname, str(e)))

    def run(self):
        # The inverter protocol suggests that communication is 'reset' every 30 seconds
        try:
            while not self.terminate.is_set():
                if (millis() - self.statetime) > self.OFFLINE_TIMEOUT:
                    self.state = self.OFFLINE

                if self.state == self.OFFLINE:
                    self._send(self.CC_REG, self.FC_QRYOFF)
                    self._send(self.CC_READ, self.FC_QRYID)
                    self.state = self.RUNNING
                    self.statetime = millis()

                self._send(self.CC_READ, self.FC_QRYRUN)

                if self.reading:
                    self._data_send(self.reading)
                    self.reading = {}

                time.sleep(10)
            logging.info('Plugin "%s" is terminating following signal' % self.myname)
        except Exception as e:
            logging.error('Exception caught %s: %s' % (type(e), e))

    def _send(self, control_code, function_code):
        try:
            self._send_udp(control_code, function_code)
            self._receive(function_code)
        except Exception as e:
            logging.error('Exception caught %s: %s' % (type(e), e))
            raise

    def _receive(self, function_code):
        try:
            # If the received response is not the one expected based on the last query sent than
            # stay in this loop processing any received data until the correct response is received
            # or a socket timeout has occured. This is done to ensure that synchronisation between
            # queries and responses is not lost due to sending delays from the inverter
            while True:
                response = self._receive_udp()
                self._check_valid_response(response)

                # No need to check index length as _check_valid_response will do this
                if response[5] == self.FC_RESRUN:
                    self._run(response)
                    if function_code == self.FC_QRYRUN:
                        break
                elif response[5] == self.FC_RESID:
                    self._id(response)
                    if function_code == self.FC_QRYID:
                        break
                elif response[5] == self.FC_RESOFF:
                    self._offline(response)
                    if function_code == self.FC_QRYOFF:
                        break
                elif response[5] == self.FC_RESIGN:
                    # Inverter likes to send an 0x86 response which may be a later than documented
                    # protocol change. This is similar to an 0x81 response in terms of data but
                    # format cannot be verified against documentation from GoodWe which is subject to NDA.
                    # This response is ignored.
                    pass
                else:
                    if function_code == self.FC_QRYOFF:
                        msg = 'offline'
                    elif function_code == self.FC_QRYID:
                        msg = 'id'
                    elif function_code == self.FC_QRYRUN:
                        msg = 'run'
                    else:
                        msg = 'unexpected'
                    raise MalFormedError('Invalid inverter response from ' + msg + ' query')
        except socket.timeout:
            pass
        except MalFormedError as e:
            logging.error(str(e))
            pass
        except Exception:
            raise  # Raise all other errors

    def _run(self, response):
        # Only get inverter information if the serial number and inverter model are known
        if self.idinfo.get('serialNumber', None) is not None and self.idinfo.get('modelName', None) is not None:
            try:
                self.reading = dict(
                    timestamp=millis(),
                    serial=self.idinfo['serialNumber'],
                    model=self.idinfo['modelName'])
                # Hard coded array indexes below are ok as the data length has been verified
                # and the indexes are defined by the protocol
                ptr = 7
                for key, value in self.single_phase.items():
                    if value[0] == self.CHAR:
                        self.reading[key] = int(response[ptr:ptr + 1])
                        ptr += 1
                    elif value[0] == self.SHORT:
                        self.reading[key] = struct.unpack('!h', response[ptr:ptr+2])
                        ptr += 2
                    elif value[0] == self.LONG:
                        self.reading[key] = struct.unpack('!i', response[ptr:ptr + 4])
                        ptr += 4
                    else:
                        pass

                    if value[1] > 1:
                        self.reading[key] = self.reading[key][0] / value[1]
                    else:
                        self.reading[key] = self.reading[key][0]
            except Exception:
                raise  # Raise all other errors

    def _id(self, response):
        try:
            # Hard coded array indexes below are ok as the data length has been verified
            # and the indexes are defined by the protocol
            response = response[7:]
            self.idinfo = dict(
                firmwareVersion=response[0:5].decode('utf-8'),
                modelName=response[5:15].decode('utf-8'),
                manufacturer=response[15:31].replace(b'\xff', b'\x20').decode('utf-8'),
                serialNumber=response[31:47].decode('utf-8'),
                nominalVpv=response[47:51].decode('utf-8'),
                internalVersion=response[51:63].decode('utf-8'),
                safetyCountryCode=response[63])
        except Exception:
            raise  # Raise all other errors

    def _offline(self, response):
        try:
            if response[5] != self.FC_RESOFF:
                raise MalFormedError('Invalid inverter response from offline query')
            # Hard coded array indexes below are ok as the data length has been verified
            # and the indexes are defined by the protocol
            self.inverter_address = response[3]
            self.ap_address = response[2]
        except Exception:
            raise  # Raise all other errors

    def _send_udp(self, control_code, function_code):
        msg = self.HEADER + bytearray([self.inverter_address, self.ap_address, control_code, function_code, 0x00])
        append_crc(msg)

        retry = 0
        while True:
            try:
                self.sock.sendto(msg, self.addr)
                # If socket send caused no errors then break out of loop otherwise
                # retry until allowed number or reties has been exceeded and then raise error
                break
            except OSError as e:
                retry += 1
                logging.info('Exception %s: %s sending data to the inverter - attempt %d' % (type(e), e, retry))
                if retry > self.SOCKET_RETRIES:
                    raise NoInverterError('Unable to send data to the inverter')
                time.sleep(self.SOCKET_RETRIES_DELAY)

    def _receive_udp(self):
        try:
            response, self.addr = self.sock.recvfrom(self.BUFFERSIZE)
            return response
        except Exception:
            raise  # Raise all other errors

    def _check_valid_response(self, buffer):
        # Hard coded array indexes below are ok as the data format and the indexes are defined by the protocol
        if len(buffer) > 6 and buffer[0:2] == self.HEADER:
            if buffer[2] == self.ap_address and buffer[3] == self.inverter_address:
                i = buffer[6] + self.PACKET_OVERHEAD
                if buffer[i - 2] * 256 + buffer[i - 1] == calculate_crc(buffer[0:i - 2]):
                    return
        raise MalFormedError('Invalid data format response received from inverter')
