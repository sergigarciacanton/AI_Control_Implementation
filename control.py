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
import asyncio
from aioquic.asyncio.server import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent, StreamDataReceived
from pathlib import Path


class Connection:
    def __init__(self, fec_id, sock, ip, gpu, ram, bw, connected_users):
        self.fec_id = fec_id
        self.sock = sock
        self.ip = ip
        self.gpu = gpu
        self.ram = ram
        self.bw = bw
        self.connected_users = connected_users

    def __str__(self):
        return f"FEC ID: {self.fec_id} | Socket ID: {self.sock} | IP: {self.ip} | " \
               f"GPU: {self.gpu} MB | RAM: {self.ram} MB | BW: {self.bw} kbps | " \
               f"Connected IDs: {self.connected_users}"


config = configparser.ConfigParser()
config.read("control_outdoor.ini")
general = config['general']

stop = False
connections = []
valid_ids = json.loads(general['valid_ids'])
vnf_list = []

logger = logging.getLogger('')
logger.setLevel(int(general['log_level']))
logger.addHandler(logging.FileHandler(general['log_file_name'], mode='w', encoding='utf-8'))
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(ColoredFormatter('%(log_color)s%(message)s'))
logger.addHandler(stream_handler)
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('quic').setLevel(logging.DEBUG)


def serve_client(conn, ip):
    global listen_fec_changes_thread
    global vnf_list
    fec_id = None
    while True:
        try:
            if stop:
                break
            data = conn.recv(1024).decode()  # Receive data stream. it won't accept data packet greater than 1024 bytes
            if not data:
                break  # If data is not received break

            if fec_id is None:
                logger.info("[I] From new FEC: " + str(data))
            else:
                logger.info("[I] From FEC " + str(fec_id) + ": " + str(data))
            json_data = json.loads(data)

            if json_data['type'] == 'id':
                print(ip)
                print(json_data)
                fec_id = 1
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
                    connections.append(Connection(fec_id, conn, json_data['ip'], 0, 0.0, 0.0, []))
                    conn.send(json.dumps(dict(res=200, id=fec_id)).encode())  # Return id
            elif json_data['type'] == 'auth':
                try:
                    if valid_ids.index(json_data['user_id']) >= 0:
                        conn.send(json.dumps(dict(res=200)).encode())
                    else:
                        conn.send(json.dumps(dict(res=403)).encode())
                except TypeError:
                    conn.send(json.dumps(dict(res=404)).encode())
                except ValueError:
                    conn.send(json.dumps(dict(res=403)).encode())
            elif json_data['type'] == 'fec':
                i = 0
                while i < len(connections):
                    if conn == connections[i].sock:
                        break
                    else:
                        i += 1
                if i == len(connections):
                    conn.send(json.dumps(dict(res=500)).encode())  # Connection not found
                else:
                    connections[i].ram = json_data['data']['ram']
                    connections[i].gpu = json_data['data']['gpu']
                    connections[i].bw = json_data['data']['bw']
                    connections[i].connected_users = json_data['data']['connected_users']
                    conn.send(json.dumps(dict(res=200)).encode())  # Success
                    notify_fec_state_changes()
            elif json_data['type'] == 'vnf':
                i = 0
                while i < len(connections):
                    if conn == connections[i].sock:
                        break
                    else:
                        i += 1
                if i == len(connections):
                    conn.send(json.dumps(dict(res=404)).encode())  # Connection not found
                else:
                    j = 0
                    while j < len(vnf_list):
                        if vnf_list[j]['user_id'] == json_data['data']['user_id']:
                            break
                        else:
                            j += 1
                    if j == len(vnf_list):
                        vnf_list.append(json_data['data'])
                    elif json_data['data']['target'] == json_data['data']['current_node']:
                        vnf_list.pop(j)
                    else:
                        vnf_list[j] = json_data['data']
                    conn.send(json.dumps(dict(res=200)).encode())  # Success
                    notify_vnf_changes()
        except TypeError:
            conn.send(json.dumps(dict(res=404)).encode())  # Error
        except json.decoder.JSONDecodeError:
            conn.send(json.dumps(dict(res=404)).encode())  # Error
        except Exception as e:
            logger.exception(e)

    found = False
    i = 0
    while not found and i < len(connections):
        if connections[i].sock == conn:
            found = True
        else:
            i += 1
    if found:
        connections.pop(i)
    conn.close()  # Close the connection
    logger.info('[I] FEC ' + str(fec_id) + ' disconnected.')
    notify_fec_state_changes()


class QUICServerProtocol(QuicConnectionProtocol):
    def quic_event_received(self, event: QuicEvent):
        if isinstance(event, StreamDataReceived):
            # parse query
            # print(event.data.decode('utf-8'))
            print(event.stream_id)

            # data = bytes('ADIOS', encoding='utf8')
            #
            # # send answer
            # self._quic.send_stream_data(event.stream_id, data, end_stream=event.end_stream)

            global listen_fec_changes_thread
            global vnf_list
            try:
                if stop:
                    return
                data = event.data.decode(
                    'utf-8')  # Receive data stream. it won't accept data packet greater than 1024 bytes
                if not data:
                    return  # If data is not received break

                logger.info("[I] From FEC: " + str(data))
                json_data = json.loads(data)

                if json_data['type'] == 'id':
                    fec_id = 1
                    fec_ips = config['fec']
                    while fec_id < len(fec_ips) + 1:
                        if fec_ips['fec_' + str(fec_id) + '_ip'] == json_data['ip']:
                            logger.info('[I] FEC ' + json_data['ip'] + ' connected! (ID: ' + str(fec_id) + ')')
                            break
                        else:
                            fec_id += 1
                    if fec_id == len(fec_ips) + 1:
                        logger.warning('[!] Unidentifiable FEC connected!')
                        self._quic.send_stream_data(event.stream_id, json.dumps(dict(res=403)).encode(),
                                                    end_stream=event.end_stream)
                    else:
                        connections.append(Connection(fec_id, fec_id, json_data['ip'], 0, 0.0, 0.0, []))
                        self._quic.send_stream_data(event.stream_id, json.dumps(dict(res=200, id=fec_id)).encode(),
                                                    end_stream=event.end_stream)
                elif json_data['type'] == 'auth':
                    try:
                        if valid_ids.index(json_data['user_id']) >= 0:
                            self._quic.send_stream_data(event.stream_id,
                                                        json.dumps(dict(res=200)).encode(),
                                                        end_stream=event.end_stream)
                        else:
                            self._quic.send_stream_data(event.stream_id,
                                                        json.dumps(dict(res=403)).encode(),
                                                        end_stream=event.end_stream)
                    except TypeError:
                        self._quic.send_stream_data(event.stream_id,
                                                    json.dumps(dict(res=404)).encode(),
                                                    end_stream=event.end_stream)
                    except ValueError:
                        self._quic.send_stream_data(event.stream_id,
                                                    json.dumps(dict(res=403)).encode(),
                                                    end_stream=event.end_stream)
                elif json_data['type'] == 'fec':
                    i = 0
                    while i < len(connections):
                        if json_data['fec_id'] == connections[i].fec_id:
                            break
                        else:
                            i += 1
                    if i == len(connections):
                        self._quic.send_stream_data(event.stream_id,
                                                    json.dumps(dict(res=500)).encode(),
                                                    end_stream=event.end_stream)  # Connection not found
                    else:
                        connections[i].ram = json_data['data']['ram']
                        connections[i].gpu = json_data['data']['gpu']
                        connections[i].bw = json_data['data']['bw']
                        connections[i].connected_users = json_data['data']['connected_users']
                        self._quic.send_stream_data(event.stream_id,
                                                    json.dumps(dict(res=200)).encode(),
                                                    end_stream=event.end_stream)  # Success
                        notify_fec_state_changes()
                elif json_data['type'] == 'vnf':
                    i = 0
                    while i < len(connections):
                        if json_data['fec_id'] == connections[i].fec_id:
                            break
                        else:
                            i += 1
                    if i == len(connections):
                        self._quic.send_stream_data(event.stream_id,
                                                    json.dumps(dict(res=404)).encode(),
                                                    end_stream=event.end_stream)  # Connection not found
                    else:
                        j = 0
                        while j < len(vnf_list):
                            if vnf_list[j]['user_id'] == json_data['data']['user_id']:
                                break
                            else:
                                j += 1
                        if j == len(vnf_list):
                            vnf_list.append(json_data['data'])
                        elif json_data['data']['target'] == json_data['data']['current_node']:
                            vnf_list.pop(j)
                        else:
                            vnf_list[j] = json_data['data']
                        self._quic.send_stream_data(event.stream_id,
                                                    json.dumps(dict(res=200)).encode(),
                                                    end_stream=event.end_stream)  # Success
                        notify_vnf_changes()
                elif json_data['type'] == 'bye':
                    found = False
                    i = 0
                    while not found and i < len(connections):
                        if json_data['fec_id'] == connections[i]:
                            found = True
                        else:
                            i += 1
                    if found:
                        connections.pop(i)
                    logger.info('[I] FEC ' + json_data['fec_id'] + ' disconnected.')
                    notify_fec_state_changes()
                else:
                    self._quic.send_stream_data(event.stream_id,
                                                json.dumps(dict(res=400)).encode(),
                                                end_stream=event.end_stream)
            except TypeError:
                self._quic.send_stream_data(event.stream_id,
                                            json.dumps(dict(res=404)).encode(),
                                            end_stream=event.end_stream)  # Error
            except json.decoder.JSONDecodeError:
                self._quic.send_stream_data(event.stream_id,
                                            json.dumps(dict(res=404)).encode(),
                                            end_stream=event.end_stream)  # Error
            except Exception as e:
                logger.exception(e)


async def init_quic_server(host, port, configuration):
    await serve(
        host,
        port,
        configuration=configuration,
        create_protocol=QUICServerProtocol,
        retry=False,
    )
    await asyncio.Future()


def notify_fec_state_changes():
    global listen_fec_changes_thread
    fec_list = []
    for connection in connections:
        fec_list.append(dict(fec_id=connection.fec_id, gpu=connection.gpu, ram=connection.ram,
                             bw=connection.bw, connected_users=connection.connected_users))
    publish('fec', json.dumps(fec_list))
    if listen_fec_changes_thread.ident is not None:
        kill_thread(listen_fec_changes_thread.ident)
    listen_fec_changes_thread = threading.Thread(target=listen_fec_changes)
    listen_fec_changes_thread.daemon = True
    listen_fec_changes_thread.start()


def listen_fec_changes():
    global connections
    previous_state = connections
    while previous_state == connections:
        time.sleep(5)
        if previous_state == connections:
            fec_list = []
            for connection in connections:
                fec_list.append(dict(fec_id=connection.fec_id, gpu=connection.gpu, ram=connection.ram,
                                     bw=connection.bw, connected_users=connection.connected_users))
            publish('fec', json.dumps(fec_list))


listen_fec_changes_thread = threading.Thread(target=listen_fec_changes)


def notify_vnf_changes():
    global listen_vnf_changes_thread
    global vnf_list
    publish('vnf', json.dumps(vnf_list))
    if listen_vnf_changes_thread.ident is not None:
        kill_thread(listen_vnf_changes_thread.ident)
    listen_vnf_changes_thread = threading.Thread(target=listen_vnf_changes)
    listen_vnf_changes_thread.daemon = True
    listen_vnf_changes_thread.start()


def listen_vnf_changes():
    global vnf_list
    previous_state = vnf_list
    while previous_state == vnf_list:
        time.sleep(5)
        if previous_state == vnf_list:
            publish('vnf', json.dumps(vnf_list))


listen_vnf_changes_thread = threading.Thread(target=listen_vnf_changes)


def publish(key, message):
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


def kill_thread(thread_id):
    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread_id), ctypes.py_object(SystemExit))
    if ret == 0:
        raise ValueError("Thread ID " + str(thread_id) + " does not exist!")
    elif ret > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
    logger.debug('[D] Successfully killed thread ' + str(thread_id))


def main():
    try:
        global stop
        stop = False
        # Server's IP and port
        host = general['control_ip']

        logger.info('[I] Control server started')

        if int(general['use_quic']) == 0:
            port = int(general['socket_port'])
            server_socket = socket.socket()  # Create socket
            server_socket.bind((host, port))  # Bind IP address and port together

            # Configure how many client the server can listen simultaneously
            server_socket.listen(1)

            # Infinite loop listening for new connections
            while True:
                conn, address = server_socket.accept()  # Accept new connection
                socket_thread = threading.Thread(target=serve_client, args=(conn, address[0]))
                socket_thread.daemon = True
                socket_thread.start()
        else:
            port = int(general['quic_port'])
            configuration = QuicConfiguration(
                alpn_protocols=["doq-i03"],
                is_client=False,
                quic_logger=None,
            )
            configuration.load_cert_chain(Path(general['server_cert_file']), Path(general['priv_key_file']))
            asyncio.run(
                init_quic_server(
                    host=host,
                    port=port,
                    configuration=configuration,
                )
            )
    except KeyboardInterrupt:
        logger.info('[!] Stopping Control server...')
        stop = True
        if int(general['use_quic']) == 0:
            for connection in connections:
                connection.sock.close()
    except OSError:
        logger.critical('[!] Error when binding address and port for server! Stopping...')
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    main()
