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
        self.fec_list = []
        self.vnf_list = []
        self.valid_ids = valid_ids
        self.listen_fec_changes_thread = threading.Thread(target=self.listen_fec_changes)
        self.listen_vnf_changes_thread = threading.Thread(target=self.listen_vnf_changes)
        self.run_control()

    def serve_client(self, conn, ip):
        fec_id = None
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
                        self.fec_list.append(dict(fec_id=fec_id, sock=conn, ip=json_data['ip'], gpu=0, ram=0.0, bw=0.0,
                                                  mac=json_data['mac'], connected_users=[]))
                        conn.send(json.dumps(dict(res=200, id=fec_id)).encode())  # Return id
                elif json_data['type'] == 'auth':
                    try:
                        if self.valid_ids.index(json_data['user_id']) >= 0:
                            conn.send(json.dumps(dict(res=200)).encode())
                        else:
                            conn.send(json.dumps(dict(res=403)).encode())
                    except TypeError:
                        conn.send(json.dumps(dict(res=404)).encode())
                    except ValueError:
                        conn.send(json.dumps(dict(res=403)).encode())
                elif json_data['type'] == 'fec':
                    i = 0
                    while i < len(self.fec_list):
                        if conn == self.fec_list[i]['sock']:
                            break
                        else:
                            i += 1
                    if i == len(self.fec_list):
                        conn.send(json.dumps(dict(res=500)).encode())  # FEC not found
                    else:
                        self.fec_list[i]['ram'] = json_data['data']['ram']
                        self.fec_list[i]['gpu'] = json_data['data']['gpu']
                        self.fec_list[i]['bw'] = json_data['data']['bw']
                        self.fec_list[i]['connected_users'] = json_data['data']['connected_users']
                        conn.send(json.dumps(dict(res=200)).encode())  # Success
                        self.notify_fec_state_changes()
                elif json_data['type'] == 'vnf':
                    i = 0
                    while i < len(self.fec_list):
                        if conn == self.fec_list[i]['sock']:
                            break
                        else:
                            i += 1
                    if i == len(self.fec_list):
                        conn.send(json.dumps(dict(res=404)).encode())  # FEC not found
                    else:
                        j = 0
                        while j < len(self.vnf_list):
                            if self.vnf_list[j]['user_id'] == json_data['data']['user_id']:
                                break
                            else:
                                j += 1
                        if j == len(self.vnf_list):
                            self.vnf_list.append(json_data['data'])
                        elif json_data['data']['target'] == json_data['data']['current_node']:
                            self.vnf_list.pop(j)
                        else:
                            self.vnf_list[j] = json_data['data']
                        conn.send(json.dumps(dict(res=200)).encode())  # Success
                        self.notify_vnf_changes()
            except TypeError:
                conn.send(json.dumps(dict(res=404)).encode())  # Error
            except json.decoder.JSONDecodeError:
                conn.send(json.dumps(dict(res=404)).encode())  # Error
            except Exception as e:
                logger.exception(e)

        found = False
        i = 0
        while not found and i < len(self.fec_list):
            if self.fec_list[i]['sock'] == conn:
                found = True
            else:
                i += 1
        if found:
            self.fec_list.pop(i)
        conn.close()  # Close the connection
        logger.info('[I] FEC ' + str(fec_id) + ' disconnected.')
        self.notify_fec_state_changes()

    def notify_fec_state_changes(self):
        fec_list = []
        for fec in self.fec_list:
            fec_list.append(dict(fec_id=fec['fec_id'], gpu=fec['gpu'], ram=fec['ram'],
                                 bw=fec['bw'], mac=fec['mac'],
                                 connected_users=fec['connected_users']))
        self.publish('fec', json.dumps(fec_list))
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
                fec_list = []
                for fec in self.fec_list:
                    fec_list.append(dict(fec_id=fec['fec_id'], gpu=fec['gpu'], ram=fec['ram'],
                                         bw=fec['bw'], mac=fec['mac'],
                                         connected_users=fec['connected_users']))
                self.publish('fec', json.dumps(fec_list))

    def notify_vnf_changes(self):
        self.publish('vnf', json.dumps(self.vnf_list))
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

            logger.info('[I] Control server started')

            # Infinite loop listening for new connections
            while True:
                conn, address = server_socket.accept()  # Accept new connection
                socket_thread = threading.Thread(target=self.serve_client, args=(conn, address[0]))
                socket_thread.daemon = True
                socket_thread.start()
        except KeyboardInterrupt:
            logger.info('[!] Stopping Control server...')
            stop = True
            for fec in self.fec_list:
                fec['sock'].close()
        except OSError:
            logger.critical('[!] Error when binding address and port for server! Stopping...')
        except Exception as e:
            logger.exception(e)


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
