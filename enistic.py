import socket


def get_number(x):
    return int(''.join(ele for ele in x if ele.isdigit()))


class Meter:
    def __init__(self, config):
        self.meter_port = config['port']
        self.meters = config['meters']

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", self.meter_port))

    def __del__(self):
        self.sock.close()

    def handle(self):
        try:
            while True:
                try:
                    data, addr = self.sock.recvfrom(4096)
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

                except Exception as e:
                    raise

        except Exception as e:
            # print("Error in '" + current_thread().name + "' " + str(type(e)) + ":" + str(e))
            raise e