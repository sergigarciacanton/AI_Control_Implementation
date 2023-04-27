import logging
import sys
import threading
import socket
import subprocess
import json
import time
import pika
import ctypes
from colorlog import ColoredFormatter


class Connection:
    def __init__(self, fec_id, sock, mac, ip, gpu, ram, bw, rtt, connected_users):
        self.fec_id = fec_id
        self.sock = sock
        self.mac = mac
        self.ip = ip
        self.gpu = gpu
        self.ram = ram
        self.bw = bw
        self.rtt = rtt
        self.connected_users = connected_users

    def __str__(self):
        return f"FEC ID: {self.fec_id} | Socket ID: {self.sock} | MAC: {self.mac} | IP: {self.ip} | " \
               f"GPU: {self.gpu} cores | RAM: {self.ram} GB | BW: {self.bw} kbps | RTT: {self.rtt} ms | " \
               f"Connected IDs: {self.connected_users}"


stop = False
connections = []
valid_ids = [1, 2, 3]
vnf_list = []

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.FileHandler('logs/control.log', mode='w', encoding='utf-8'))
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(ColoredFormatter())
logger.addHandler(stream_handler)
logging.getLogger('pika').setLevel(logging.WARNING)


def serve_client(conn, ip):
    global listen_fec_changes_thread
    global vnf_list
    fec_id = 0
    if ip == '147.83.118.154':
        fec_id = 1
    elif ip == '147.83.118.82':
        fec_id = 2
    else:
        logger.critical('[!] Unidentifiable FEC connected!')
    connections.append(Connection(fec_id,
                                  conn,
                                  subprocess.check_output(['arp', '-n', ip]).decode().split('\n')[
                                      1].split()[2],
                                  ip,
                                  0, 0.0, 0.0, 0, []))
    while True:
        try:
            if stop:
                break
            data = conn.recv(1024).decode()  # Receive data stream. it won't accept data packet greater than 1024 bytes
            if not data:
                break  # If data is not received break

            logger.info("[I] From FEC " + str(ip) + ": " + str(data))
            json_data = json.loads(data)

            if json_data['type'] == 'id':
                i = 0
                while i < len(connections):
                    if conn == connections[i].sock:
                        conn.send(json.dumps(dict(res=200, id=connections[i].fec_id)).encode())  # Return id
                        break
                    else:
                        i += 1
                if i == len(connections):
                    conn.send(json.dumps(dict(res=500)).encode())  # Connection not found
            elif json_data['type'] == 'auth':
                try:
                    if valid_ids.index(json_data['user_id']) >= 0:
                        conn.send(json.dumps(dict(res=200)).encode())
                    else:
                        conn.send(json.dumps(dict(res=403)).encode())
                except TypeError:
                    conn.send(json.dumps(dict(res=404)).encode())
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
                    connections[i].rtt = json_data['data']['rtt']
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
                        print(vnf_list[j])
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
    logger.info('[I] FEC ' + str(ip) + ' disconnected.')
    notify_fec_state_changes()


def notify_fec_state_changes():
    global listen_fec_changes_thread
    fec_list = []
    for connection in connections:
        fec_list.append(dict(fec_id=connection.fec_id, gpu=connection.gpu, ram=connection.ram,
                             bw=connection.bw, rtt=connection.rtt, connected_users=connection.connected_users))
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
                                     bw=connection.bw, rtt=connection.rtt, connected_users=connection.connected_users))
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
    rabbit_conn = pika.BlockingConnection(
        pika.ConnectionParameters('147.83.118.153', credentials=pika.PlainCredentials('sergi', 'EETAC2023')))
    try:
        channel = rabbit_conn.channel()

        channel.exchange_declare(exchange='test', exchange_type='direct')

        channel.basic_publish(
            exchange='test', routing_key=key, body=message)
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
        host = '147.83.118.153'
        port = 5000

        server_socket = socket.socket()  # Create socket
        server_socket.bind((host, port))  # Bind IP address and port together

        # Configure how many client the server can listen simultaneously
        server_socket.listen(1)

        logger.info('[I] Control server started')

        # Infinite loop listening for new connections
        while True:
            conn, address = server_socket.accept()  # Accept new connection
            logger.info("[I] New connection from: " + str(address))
            socket_thread = threading.Thread(target=serve_client, args=(conn, address[0]))
            socket_thread.daemon = True
            socket_thread.start()
    except KeyboardInterrupt:
        logger.info('[!] Stopping Control server...')
        stop = True
        for connection in connections:
            connection.sock.close()
    except OSError:
        logger.critical('[!] Error when binding address and port for server! Stopping...')
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    main()
