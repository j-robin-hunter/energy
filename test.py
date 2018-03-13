import json
import socket
from urllib import request
from urllib.parse import urlparse
from queue import Queue
from threading import Thread, current_thread
import os
import argparse
from goodwe import Inverter
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
    )

db_queue = Queue()


def read_config(config):
    parse_result = urlparse(config)

    try:
        if parse_result.scheme in ('http', 'https'):
            response = request.urlopen(config)
            config_data = response.read()
        else:
            with open(config) as configFile:
                config_data = configFile.read()
        return json.loads(config_data)

    except Exception as e:
        print(str(type(e)) + ": ", end='')
        raise


def verify_database(config):
    try:
        host_url = config["influxdb"]["hosturl"]
        request.urlopen(host_url + 'query?q=create+database+"' + config["influxdb"]["database"] + '"')

        return config["influxdb"]["hosturl"] + \
            "write?db=" + config["influxdb"]["database"] + \
            "&u=" + config["influxdb"]["username"] + \
            "&p=" + config["influxdb"]["password"]
    except Exception as e:
        print(str(type(e)) + ": ", end='')
        raise


def get_number(x):
    return int(''.join(ele for ele in x if ele.isdigit()))


def process_data(url):
    try:
        while True:
            meter_reading = db_queue.get()
            post_data = "reading,name=" + str(meter_reading["name"]).replace(" ", "\ ") + \
                ",category=" + str(meter_reading["category"]).replace(" ", "\ )") + \
                " value=" + str(meter_reading["power"])
            post_data = post_data.encode('utf-8')
            req = request.Request(url, post_data)
            request.urlopen(req)

    except Exception as e:
        print("Error in '" + current_thread().name + "' " + str(type(e)) + ":" + str(e))
        os._exit(-1)


def read_meter_data(enistic_config):
    try:
        meter_port = enistic_config['port']
        meters = enistic_config['meters']

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", meter_port))

        while True:
            try:
                data, addr = sock.recvfrom(4096)
                index = data.find(b"D1")
                if index > 0:
                    tokens = data[index:len(data)].decode("utf-8").split(",")
                    if len(tokens) >= 6:
                        meter_reading = {"power": get_number(tokens[3])}
                        meter = list(filter(lambda meter: meter['channel'] == get_number(tokens[5]), meters))
                        try:
                            meter_reading["name"] = meter[0].get("name")
                            meter_reading["category"] = meter[0].get("category")

                        except IndexError:
                            meter_reading["name"] = "Channel " + str(get_number(tokens[5]))
                            meter_reading["category"] = "General"

                        finally:
                            db_queue.put(meter_reading)

            except Exception as e:
                raise

    except Exception as e:
        print("Error in '" + current_thread().name + "' " + str(type(e)) + ":" + str(e))
        os._exit(-1)


def read_inverter_data(goodwe_config):
    try:
        inverter_comms = Inverter(goodwe_config)
        while True:
            inverter_comms.handle()

    except Exception as e:
        print("Error in '" + current_thread().name + "' " + str(type(e)) + ":" + str(e))
        os._exit(-1)


try:
    parser = argparse.ArgumentParser(prog="home_logger",
        description="An energy data logger Energy data logger program for my home")
    parser._positionals.title = "arguments"
    parser.add_argument("config", help="Full pathname or URL to the configuration file")
    args = parser.parse_args()

    print("Reading configuration....", end='')
    config = read_config(args.config)
    print("ok")

    print("Verifying database connectivity....", end='')
    #influxdb_url = verify_database(config)
    print("ok")

    threads = []  # [Thread(name='InfluxDB worker', target=process_data, args=[influxdb_url])]
    if "enistic" in config:
        threads.append(Thread(name='Enistic smart meter worker', target=read_meter_data, args=[config['enistic']]))
    if "goodwe" in config:
        threads.append(Thread(name='GoodWe inverter worker', target=read_inverter_data, args=[config['goodwe']]))

    for thread in threads:
        print("Starting %s" % thread.name + "....", end='')
        thread.start()
        print("ok")

    print("<--->")
    thread.join()

except Exception as e:
    print(str(e))
