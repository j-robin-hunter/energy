import socket
import struct

def calculateCRC(buffer):
    crc = 0
    for i in range(len(buffer)):
        crc += buffer[i]
    buffer.extend(crc.to_bytes(2, byteorder='big'))


def printFloat(val, text, divisor):
    print("0x%02x%02x %s=%.2f" % (val[0], val[1], text, ((val[0] << 8 | val[1])/divisor)))


def printByte(val,text):
    print("0x%02x %s=%d" % (val, text, val))


def printUnknown(val, offset):
    print("0x%02x%02x Unknown @ %d - %d %d %.2f" % (val[offset], val[offset + 1], offset, val[offset], val[offset + 1], (val[offset] << 8 | val[offset + 1])))


try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #data = bytearray(b'\xaa\x55\xb0\x7f\x01\x02\x00\x02\x31')
    data = bytearray([0xaa, 0x55, 0x80, 0x7f, 0x00, 0x00, 0x00])
    calculateCRC(data)
    sock.sendto(data, ('192.168.1.152', 8899))
    print("try read 1")
    data, addr = sock.recvfrom(4096)
    print(data)

    data = bytearray(b'\xaa\x55\x80\x7f\x01\x01\x00\x02\x00')
    sock.sendto(data, ('192.168.1.152', 8899))
    print("try read 2")
    data, addr = sock.recvfrom(4096)

    #up = struct.unpack(">e", data[7:9])
    #print(up)
    f = open("C:/Users/Robin Hunter/Downloads/hex.txt", "w")
    for i in range(len(data)):
        s = "%02x\n" % data[i]
        f.write(s)
    f.close()
    print('')
    print('Data count = %d' % data[6])

    printFloat(data[7:9], 'Vpv1(V)', 10)
    printFloat(data[9:11], 'Ipv1(I)', 10)
    printFloat(data[11:13], 'Maybe Pv1_power(kW)', 10)
    printFloat(data[13:15], 'Vpv2(V)', 10)
    printFloat(data[15:17], 'Ipv2(I)', 10)
    printFloat(data[17:19], 'Maybe Pv2_power(kW)', 10)
    printFloat(data[27:29], 'BMS_Charge_I_Max', 1)
    printFloat(data[29:31], 'BMS_Discharge_I_Max', 1)
    printByte(data[33], 'BMS_Temperature')
    printByte(data[34], 'Unknown')
    printByte(data[35], 'SOC(%)')
    printByte(data[36], 'SOH(%)')
    printByte(data[37], 'Battery_Work_Mode')
    printFloat(data[41:43], 'Vac1(V)', 10)
    printFloat(data[43:45], 'Iac1(A)', 10)
    printFloat(data[45:47], 'PGrid(W)', 1)
    printFloat(data[47:49], 'Fac1(Hz)', 100)
    printFloat(data[54:56], 'LoadPower(W)', 1)
    printFloat(data[60:62], 'Temperature(C)', 10)
    printFloat(data[68:70], 'Etotal(kWh)', 10)
    printFloat(data[72:74], 'Htotal(Hour)', 1)
    printFloat(data[74:76], 'Eday', 10)
    printFloat(data[76:78], 'E_Load_Day(kWh)', 10)
    printFloat(data[80:82], 'E_Total_Load(kWh)', 10)

    printUnknown(data, 20)
    printUnknown(data, 21)
    printUnknown(data, 22)
    printUnknown(data, 23)
    printUnknown(data, 24)
    printUnknown(data, 25)
    printUnknown(data, 26)
    printUnknown(data, 27)
    printUnknown(data, 29)
    printUnknown(data, 31)
    printUnknown(data, 32)
    printUnknown(data, 38)
    printUnknown(data, 39)
    printUnknown(data, 40)


except Exception as e:
    print(e)