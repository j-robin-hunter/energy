import json
from pprint import pprint
import socket

def readConfig(file):
  with open(file) as configFile:
    config = json.loads(configFile.read())

  return config['meters']

def getNum(x):
    return int(''.join(ele for ele in x if ele.isdigit()))

def readData(meters):
  sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sock.bind(("0.0.0.0", 53005))
  while True:
    try:
      data, addr = sock.recvfrom(4096)
      index = data.find(b"D1")
      if index > 0:
        tokens = data[index:len(data)].decode("utf-8").split(",")
        if len(tokens) >= 6:
          power = getNum(tokens[3])
          meter = list(filter(lambda meter: meter['channel'] == getNum(tokens[5]), meters))
          print("%s = %d watts, contributing to %s" % (meter[0].get("name"), power / 1000, meter[0].get("category")))

    except Exception as e:
        print(e)

readData(readConfig("C:\\Users\\Robin Hunter\\PycharmProjects\\energy\\config.json"))