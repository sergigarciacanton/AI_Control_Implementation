import logging
import sys
import threading
import socket
import json
import time
import pika
import ctypes
from colorlog import ColoredFormatter
import configparser

class ControlServer:
    def __init__(self, valid_ids):
        self.fec_list = dict()
        self.vnf_list = dict()
        self.valid_ids = valid_ids
        self.listen_fec_changes_thread = threading.Thread(target=self.listen_fec_changes)
        self.listen_vnf_changes_thread = threading.Thread(target=self.listen_vnf_changes)
        self.test_rabbitmq_thread = threading.Thread(target=self.subscribe)
        self.run_control()
        self.start_time = 0

    def serve_client(self, conn):
        fec_id = None
        data = None
        json_data = None
        while True:
            try:
                if stop:
                    break
                data = conn.recv(1024).decode()  # Receive data stream. it won't accept data packet greater than 1024 B
                if not data:
                    break  # If data is not received break
                if fec_id is None:
                    logger.info("[I] From new FEC: " + str(data))
                else:
                    logger.info("[I] From FEC " + str(fec_id) + ": " + str(data))
                json_data = json.loads(data)

                if json_data['type'] == 'id':
                    fec_id = 0
                    fec_ips = config['fec']
                    while fec_id < len(fec_ips):
                        if fec_ips['fec_' + str(fec_id) + '_ip'] == json_data['ip']:
                            logger.info('[I] FEC ' + json_data['ip'] + ' connected! (ID: ' + str(fec_id) + ')')
                            break
                        else:
                            fec_id += 1
                    if fec_id == len(fec_ips) + 1:
                        logger.warning('[!] Unidentifiable FEC connected!')
                        conn.send(json.dumps(dict(res=403)).encode())  # Return id
                    else:
                        self.fec_list[fec_id] = {"sock": conn, "ip": json_data['ip'], "gpu": 0, "ram": 0.0,
                                                      "bw": 0.0, "connected_users": []}
                        conn.send(json.dumps(dict(res=200, id=fec_id)).encode())  # Return id
                elif json_data['type'] == 'auth':
                    try:
                        if self.valid_ids.index(json_data['user_id']) >= 0:
                            self.update_prometheus_target(self.fec_list[fec_id]['ip'] + ':9000')
                            conn.send(json.dumps(dict(res=200)).encode())
                        else:
                            conn.send(json.dumps(dict(res=403)).encode())
                    except TypeError as e:
                        logger.error(
                            '[!] TypeError when authentication of CAV: ' + str(e) + '. json_data = ' + str(json_data))
                        conn.send(json.dumps(dict(res=404)).encode())
                    except ValueError as e:
                        logger.error(
                            '[!] ValueError when authentication of CAV: ' + str(e) + '. json_data = ' + str(json_data))
                        conn.send(json.dumps(dict(res=403)).encode())
                elif json_data['type'] == 'fec':
                    self.fec_list[fec_id]['ram'] = json_data['data']['ram']
                    self.fec_list[fec_id]['gpu'] = json_data['data']['gpu']
                    self.fec_list[fec_id]['bw'] = json_data['data']['bw']
                    self.fec_list[fec_id]['connected_users'] = json_data['data']['connected_users']
                    conn.send(json.dumps(dict(res=200)).encode())  # Success
                    self.notify_fec_state_changes()
                elif json_data['type'] == 'vnf':
                    if json_data['user_id'] not in self.vnf_list:
                        self.vnf_list[json_data['user_id']] = json_data['data']
                    elif json_data['data']['target'] == json_data['data']['current_node']:
                        self.vnf_list.pop(json_data['user_id'])
                    else:
                        self.vnf_list[json_data['user_id']] = json_data['data']
                    conn.send(json.dumps(dict(res=200)).encode())  # Success
                    self.notify_vnf_changes()
                elif json_data['type'] == 'latency':
                    json_data[0] = json_data['data']
                    json_data[1] = time.time()
                    self.publish('latency', json.dumps(json_data))
                    logger.info('Sent latency message to RabbitMQ')

            except TypeError as e:
                logger.error('[!] TypeError in control! ' + str(e))
                conn.send(json.dumps(dict(res=404)).encode())  # Error
            except json.decoder.JSONDecodeError as e:
                logger.error('[!] JSONDecodeError in control: ' + str(e) + '. data = ' + str(data) + '. json_data = '
                             + str(json_data))
                conn.send(json.dumps(dict(res=404)).encode())  # Error
            except Exception as e:
                logger.exception(e)

        self.fec_list.pop(fec_id)
        conn.close()  # Close the connection
        logger.info('[I] FEC ' + str(fec_id) + ' disconnected.')
        self.notify_fec_state_changes()

    def notify_fec_state_changes(self):
        list_to_send = dict()
        for fec in self.fec_list.keys():
            list_to_send[fec] = dict(self.fec_list[fec])
            del list_to_send[fec]['sock']
        self.publish('fec', json.dumps(list_to_send))
        if general['retransmit_if'] != 'n' and general['retransmit_if'] != 'N':
            if self.listen_fec_changes_thread.ident is not None:
                self.kill_thread(self.listen_fec_changes_thread.ident)
            self.listen_fec_changes_thread = threading.Thread(target=self.listen_fec_changes)
            self.listen_fec_changes_thread.daemon = True
            self.listen_fec_changes_thread.start()

    def listen_fec_changes(self):
        previous_state = self.fec_list
        while previous_state == self.fec_list:
            time.sleep(5)
            if previous_state == self.fec_list:
                list_to_send = self.fec_list.copy()
                if general['wifi_if'] != 'n' and general['wifi_if'] != 'N':
                    for fec in self.fec_list.keys():
                        del list_to_send[fec]['sock']
                        del list_to_send[fec]['ip']
                    self.publish('fec', json.dumps(list_to_send))
                else:
                    for fec in self.fec_list.keys():
                        del list_to_send[fec]['sock']
                    self.publish('fec', json.dumps(list_to_send))

    def notify_vnf_changes(self):
        self.publish('vnf', json.dumps(self.vnf_list))
        if general['retransmit_if'] != 'n' and general['retransmit_if'] != 'N':
            if self.listen_vnf_changes_thread.ident is not None:
                self.kill_thread(self.listen_vnf_changes_thread.ident)
            self.listen_vnf_changes_thread = threading.Thread(target=self.listen_vnf_changes)
            self.listen_vnf_changes_thread.daemon = True
            self.listen_vnf_changes_thread.start()

    def listen_vnf_changes(self):
        previous_state = self.vnf_list
        while previous_state == self.vnf_list:
            time.sleep(5)
            if previous_state == self.vnf_list:
                self.publish('vnf', json.dumps(self.vnf_list))

    def publish(self, key, message):
        rabbit_conn = pika.BlockingConnection(pika.ConnectionParameters(general['control_ip'],
                                                                        credentials=pika.PlainCredentials(
                                                                            general['control_username'],
                                                                            general['control_password'])))
        try:
            channel = rabbit_conn.channel()

            channel.exchange_declare(exchange=general['control_exchange_name'], exchange_type='direct')
            self.start_time = time.time()
            channel.basic_publish(
                exchange=general['control_exchange_name'], routing_key=key, body=message)
            logger.debug("[D] Published message. Key: " + key + ". Message: " + message)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.exception(e)
        finally:
            rabbit_conn.close()

    def kill_thread(self, thread_id):
        ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread_id), ctypes.py_object(SystemExit))
        if ret == 0:
            raise ValueError("Thread ID " + str(thread_id) + " does not exist!")
        elif ret > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
        logger.debug('[D] Successfully killed thread ' + str(thread_id))

    def run_control(self):
        try:
            global stop
            stop = False

            # Server's IP and port
            host = general['control_ip']
            port = int(general['server_port'])

            server_socket = socket.socket()  # Create socket
            server_socket.bind((host, port))  # Bind IP address and port together

            # Configure how many client the server can listen simultaneously
            server_socket.listen(1)

            logger.warning('[I] Control server started')
            self.test_rabbitmq_thread.start()

            # Infinite loop listening for new connections
            while True:
                conn, address = server_socket.accept()  # Accept new connection
                socket_thread = threading.Thread(target=self.serve_client, args=(conn,))
                socket_thread.daemon = True
                socket_thread.start()
        except KeyboardInterrupt:
            logger.warning('[!] Stopping Control server...')
            stop = True
            for fec in self.fec_list:
                fec['sock'].close()
        except OSError:
            logger.critical('[!] Error when binding address and port for server! Stopping...')
        except Exception as e:
            logger.exception(e)

    def update_prometheus_target(self, cav_ip):
        print("Canvi Prometheus a " + cav_ip)
        with open('/home/user/Downloads/prometheus-2.51.2.linux-amd64/targets.json', 'r') as json_file:
            prom_target = json.load(json_file)
        prom_target[0]["targets"] = [cav_ip]
        with open('/home/user/Downloads/prometheus-2.51.2.linux-amd64/targets.json', 'w') as json_file:
            json.dump(prom_target, json_file, indent=4)
        json_file.close()

    def subscribe(self):
        rabbit_conn_down = pika.BlockingConnection(pika.ConnectionParameters("10.2.20.1",
                                                                             credentials=pika.PlainCredentials(
                                                                                 "sergi",
                                                                                 "EETAC2023")))
        channel_down = rabbit_conn_down.channel()
        channel_down.exchange_declare(exchange='test', exchange_type='direct')
        result = channel_down.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel_down.queue_bind(exchange='test', queue=queue_name, routing_key='latency')

        def callback(ch, method, properties, body):
            start_time1 = float(json.loads(body.decode('utf-8'))['1'])
            print(f"Time elapsed for specific message is {(time.time() - start_time1) * 1000:.3f} ms")

        channel_down.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        channel_down.start_consuming()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("control.ini")
    general = config['general']

    stop = False

    logger = logging.getLogger('')
    logger.setLevel(int(general['log_level']))
    logger.addHandler(logging.FileHandler(general['log_file_name'], mode='w', encoding='utf-8'))
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(ColoredFormatter('%(log_color)s%(message)s'))
    logger.addHandler(stream_handler)
    logging.getLogger('pika').setLevel(logging.WARNING)

    my_control = ControlServer(json.loads(general['valid_ids']))
