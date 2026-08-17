import socket
import select

def build_reply(command: list, store: dict):
    if not command:
        return b"-ERR empty command\r\n"
    cmd_name = command[0].upper()
    if cmd_name == "PING":
        return b"+PONG\r\n"
    if cmd_name == "ECHO":
        txt = command[1]
        return f"${len(txt)}\r\n{txt}\r\n".encode()
    if cmd_name == "GET":
        key = command[1]
        value = store.get(key)
        if value is None:
            return b"$-1\r\n"
        return f"${len(value)}\r\n{value}\r\n".encode()
    if cmd_name == "SET":
        key = command[1]
        value = command[2]
        store[key] = value
        return b"+OK\r\n"
    else:
        return b"-ERR unknown command\r\n"

def parse_command(data: bytes):
    input_string = data.decode()
    lines = input_string.split('\r\n')
    command = []
    for line in lines:
        if line == "":
            continue
        if line.startswith("*") or line.startswith("$"):
            continue
        command.append(line)
    return command

def main():
    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )
    server_socket.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server_socket.bind(("localhost", 6379))
    server_socket.listen()

    sockets_to_monitor = [server_socket]

    store = {}
    while True:
        readable, _, _ = select.select(sockets_to_monitor, [], [])

        for sock in readable:
            if sock is server_socket:
                conn, add = server_socket.accept()
                sockets_to_monitor.append(conn)
            else:
                data = sock.recv(1024)

                if data == b"":
                    sockets_to_monitor.remove(sock)
                    sock.close()
                else:
                    command = parse_command(data)
                    response = build_reply(command, store)
                    sock.send(response)

if __name__ == "__main__":
    main()