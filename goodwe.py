from enum import Enum
import time
import socket
import ipaddress
import struct
import logging


def millis():
    return int(round(time.time() * 1000))


def tofloat(buffer):
    val = (buffer[0] & 0x7f) << 8 | buffer[1]
    if buffer[0] > 0x7f:
        val *= -1
    return val


def tofloat4(buffer):
    val = (buffer[0] & 0x7f) << 24 | buffer[1] << 16 | buffer[2] << 8 | buffer[3]
    if buffer[0] > 0x7f:
        val *= -1
    return val


class MalFormedError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class IDInfo(object):
    def __init__(self):
        self.firmwareVersion = ""
        self.modelName = ""
        self.manufacturer = ""
        self.serialNumber = ""
        self.nominalVpv = 0
        self.internalVersion = ""
        self.safetyCountryCode = 0x0


class DataTypes(Enum):
    FLOAT = 0
    FLOAT4 = 1
    SHORT = 3
    CHAR = 4
    STRING = 5
    LONG = 6


class RunInfo(object):
    single_phase = {
        "Vpv1": [DataTypes.FLOAT, 10],
        "Vpv2": [DataTypes.FLOAT, 10],
        "Ipv1": [DataTypes.FLOAT, 10],
        "Ipv2": [DataTypes.FLOAT, 10],
        "Vac1": [DataTypes.FLOAT, 10],
        "Iac1": [DataTypes.FLOAT, 10],
        "Fac1": [DataTypes.FLOAT, 100],
        "PGrid": [DataTypes.FLOAT, 10],
        "WorkMode": [DataTypes.SHORT, 1],
        "Temperature": [DataTypes.FLOAT, 10],
        "ErrorMessage": [DataTypes.LONG, 1],
        "ETotal": [DataTypes.FLOAT4, 1],
        "HTotal": [DataTypes.FLOAT4, 1],
        "SoftVersion": [DataTypes.SHORT, 1],
        "WarningCode": [DataTypes.SHORT, 1],
        "PV2FaultValue": [DataTypes.FLOAT, 10],
        "FunctionsBitValue": [DataTypes.SHORT, 1],
        "BUSVoltage": [DataTypes.FLOAT, 10],
        "GFCICheckValue_SafetyCountry": [DataTypes.SHORT, 1],
        "EDay": [DataTypes.FLOAT, 10],
        "Vbattery1": [DataTypes.FLOAT, 10],
        "Ibattery1": [DataTypes.FLOAT, 10],
        "SOC1": [DataTypes.FLOAT, 1],
        "Errorcode": [DataTypes.SHORT, 1],
        "PVTotal": [DataTypes.FLOAT, 10],
        "LoadPower": [DataTypes.FLOAT4, 1],
        "E_Load_Day": [DataTypes.FLOAT, 10],
        "E_Total_Load": [DataTypes.FLOAT4, 10],
        "InverterPower": [DataTypes.FLOAT, 1],
        "Vload": [DataTypes.FLOAT, 10],
        "Iload": [DataTypes.FLOAT, 10],
        "OperationMode": [DataTypes.SHORT, 1],
        "BMS_Alarm": [DataTypes.SHORT, 1],
        "BMS_Warning": [DataTypes.SHORT, 1],
        "SOH": [DataTypes.FLOAT, 1],
        "BMS_Temperature": [DataTypes.FLOAT, 10],
        "BMS_Charge_I_Max": [DataTypes.FLOAT, 1],
        "BMS_Discharge_I_Max": [DataTypes.FLOAT, 1],
        "Battery_Work_Mode": [DataTypes.SHORT, 1],
        "Pmeter": [DataTypes.FLOAT, 10]
    }
    three_phase = {}

    def __init__(self):
        self.reading = dict()


class State(Enum):
    OFFLINE = 1
    CONNECTED = 2
    RUNNING = 9


# MESSAGE HEADER
HEADER = bytearray([0xaa, 0x55])

# CONTROL CODES
CC_REG = 0x00
CC_READ = 0x01

# REGISTER FUNCTION CODES
FC_QRYOFF = 0x00
FC_RESOFF = 0x80
FC_QRYID = 0x02
FC_RESID = 0x82
FC_QRYRUN = 0x01
FC_RESRUN = 0x81


class Inverter:
    BUFFERSIZE = 1024
    AP_ADDRESS = 0x7f  # our address
    INVERTER_ADDRESS = 0x80  # inverter address. We only have one inverter using USB.
    OFFLINE_TIMEOUT = 30000  # Re-verify connectivity and ID info
    DISCOVERY_INTERVAL = 10000  # 10 secs between discovery
    PAUSE = 1000  # Re-query pause time

    def __init__(self, config):
        self.config = config
        self.state = State.OFFLINE
        self.statetime = millis()
        self.lastReceived = millis()

        self.idinfo = IDInfo()
        self.runinfo = RunInfo()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(10.0)

        try:
            self.addr = str(
                ipaddress.IPv4Network(self.config['host'], strict=False).broadcast_address,
                self.config['port'])
        except Exception:
            raise RuntimeError('Unable to locate inverter on network')

    def __del__(self):
        self.sock.close()

    def handle(self):
        try:
            if (millis() - self.statetime) > self.OFFLINE_TIMEOUT:
                self.state = State.OFFLINE

            if self.state == State.OFFLINE:
                self.offline_query()
            elif self.state == State.CONNECTED:
                self.query_id()
            else:
                self.query_run()
                time.sleep(self.DISCOVERY_INTERVAL / 1000)
        except MalFormedError as e:
            logging.info(e.__str__())
            pass  # Ignore malformed response errors
        except Exception:
            raise

    def query_run(self):
        try:
            response = self._send(CC_READ, FC_QRYRUN, [])
            if response[5] != FC_RESRUN:
                raise MalFormedError('Invalid inverter response from run query')
            self.runinfo.timestamp = millis()

            # Hard coded array indexes below are ok as the data length has been verified
            # and the indexes are defined by the protocol
            ptr = 7
            for key, value in self.runinfo.single_phase.items():
                if value[0] == DataTypes.FLOAT:
                    self.runinfo.reading[key] = tofloat(response[ptr:]) / value[1]
                    ptr += 2
                elif value[0] == DataTypes.FLOAT4:
                    self.runinfo.reading[key] = tofloat4(response[ptr:]) / value[1]
                    ptr += 4
                elif value[0] == DataTypes.SHORT:
                    # ignore any divisor
                    self.runinfo.reading[key] = int(tofloat(response[ptr:]))
                    ptr += 2
                elif value[0] == DataTypes.CHAR:
                    self.runinfo.reading[key] = response[ptr:ptr + 1]
                    ptr += 1
                elif value[0] == DataTypes.LONG:
                    # Ignore any divisor
                    self.runinfo.reading[key] = int(tofloat4(response[ptr:]))
                    ptr += 4
                else:
                    pass

            time.sleep(self.DISCOVERY_INTERVAL / 1000)
        except socket.timeout as e:
            logging.info(e.__str__())
            pass  # Ignore timeout errors
        except Exception:
            raise  # Raise all other errors

    def query_id(self):
        try:
            response = self._send(CC_READ, FC_QRYID, [])
            if response[5] != FC_RESID:
                raise MalFormedError('Invalid inverter response from id query')

            # Hard coded array indexes below are ok as the data length has been verified
            # and the indexes are defined by the protocol
            response = response[7:]
            self.idinfo.firmwareVersion = "".join(map(chr, response[0:5]))
            self.idinfo.modelName = "".join(map(chr, response[5:15]))
            self.idinfo.manufacturer = "".join(map(chr, response[15:31]))
            self.idinfo.serialNumber = "".join(map(chr, response[31:47]))
            self.idinfo.nominalVpv = "".join(map(chr, response[47:51]))
            self.idinfo.internalVersion = "".join(map(chr, response[51:63]))
            self.idinfo.safetyCountryCode = response[63]

            self.state = State.RUNNING
            time.sleep(self.PAUSE / 1000)
        except socket.timeout as e:
            logging.info(e.__str__())
            pass  # Ignore timeout errors
        except Exception:
            raise  # Raise all other errors

    def offline_query(self):
        try:
            response = self._send(CC_REG, FC_QRYOFF, [])
            if response[5] != FC_RESOFF:
                raise MalFormedError('Invalid inverter response from offline query')
        except socket.timeout:
            raise RuntimeError('Unable to locate inverter on network')
        except Exception:
            raise  # Raise all other errors

        # Hard coded array indexes below are ok as the data length has been verified
        # and the indexes are defined by the protocol
        self.INVERTER_ADDRESS = response[3]
        self.state = State.CONNECTED
        self.statetime = millis()
        time.sleep(self.PAUSE / 1000)

    def _send(self, control_code, function_code, data=None):
        msg = HEADER + bytearray([self.INVERTER_ADDRESS, self.AP_ADDRESS, control_code, function_code, len(data)])
        if len(data) > 0:
            msg.extend(data)
        self._append_crc(msg)

        try:
            self.sock.sendto(msg, self.addr)
            response, self.addr = self.sock.recvfrom(self.BUFFERSIZE)
            self._check_valid_response(response)
            return response
        except Exception:
            raise  # Raise all other errors

    @staticmethod
    def _calculate_crc(buffer):
        crc = 0
        for i in range(len(buffer)):
            crc += buffer[i]
        return crc

    def _append_crc(self, buffer):
        buffer.extend(self._calculate_crc(buffer).to_bytes(2, byteorder='big'))

    def _check_valid_response(self, buffer):
        if len(buffer) > 4 and buffer[0:2] == HEADER:
            if struct.unpack('>h', buffer[len(buffer) - 2:len(buffer)])[0] == self._calculate_crc(
                    buffer[0:len(buffer) - 2]):
                return

        raise MalFormedError('Invalid data format response received from inverter')
