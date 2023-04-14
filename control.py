import threading
import socket
import subprocess
import json
import time
import pika
import ctypes

stop = False
connections = []
valid_ids = [1, 2, 3]


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
        return "FEC ID: " + self.fec_id + " | Socket ID: " + self.sock + " | MAC: " + self.mac + " | IP: " + self.ip + \
            " | GPU: " + self.gpu + " cores | RAM: " + self.ram + " GB | BW: " + self.bw + " kbps" \
                                                                                           " | RTT: " + self.rtt + " ms"


def publish(key, message):
    rabbit_conn = pika.BlockingConnection(
        pika.ConnectionParameters('147.83.118.153', credentials=pika.PlainCredentials('sergi', 'EETAC2023')))
    try:
        channel = rabbit_conn.channel()

        channel.exchange_declare(exchange='test', exchange_type='direct')

        channel.basic_publish(
            exchange='test', routing_key=key, body=message)
        print("[I] Published message. Key: " + key + ". Message: " + message)
    except KeyboardInterrupt:
        pass
    finally:
        rabbit_conn.close()


def serve_client(conn, ip, fec_id):
    global listen_changes_thread
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

            print("[I] From FEC " + str(ip) + ": " + str(data))
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
                    notify_state_changes()
        except TypeError:
            conn.send(json.dumps(dict(res=404)).encode())  # Error
        except json.decoder.JSONDecodeError:
            conn.send(json.dumps(dict(res=404)).encode())  # Error
        except ConnectionResetError:
            pass

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
    print('[I] FEC ' + str(ip) + ' disconnected.')
    notify_state_changes()


def notify_state_changes():
    global listen_changes_thread
    fec_list = []
    for connection in connections:
        fec_list.append(dict(fec_id=connection.fec_id, gpu=connection.gpu, ram=connection.ram,
                             bw=connection.bw, rtt=connection.rtt, connected_users=connection.connected_users))
    publish('fec', json.dumps(fec_list))
    if listen_changes_thread.ident is not None:
        kill_thread(listen_changes_thread.ident)
    listen_changes_thread = threading.Thread(target=listen_fec_changes)
    listen_changes_thread.daemon = True
    listen_changes_thread.start()


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


def kill_thread(thread_id):
    ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(thread_id), ctypes.py_object(SystemExit))
    if ret == 0:
        raise ValueError("Thread ID " + str(thread_id) + " does not exist!")
    elif ret > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
    print('[I] Successfully killed thread ' + str(thread_id))


listen_changes_thread = threading.Thread(target=listen_fec_changes)


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

        current_id = 0
        print('[I] Control server started')

        # Infinite loop listening for new connections
        while True:
            conn, address = server_socket.accept()  # Accept new connection
            print("[I] New connection from: " + str(address))
            socket_thread = threading.Thread(target=serve_client, args=(conn, address[0], current_id))
            socket_thread.daemon = True
            socket_thread.start()
            current_id += 1
    except KeyboardInterrupt:
        print('[!] Stopping Control server...')
        stop = True
        for connection in connections:
            connection.sock.close()


if __name__ == '__main__':
    main()
