#!/usr/bin/env python3
"""
Local SOCKS5 Proxy Server
=========================
Run this on your LOCAL Mac with ExpressVPN connected.
It creates a SOCKS5 proxy that can be forwarded to UCloud via SSH.
"""

import socket
import select
import threading
import struct
import sys

class SOCKS5Server:
    def __init__(self, host='127.0.0.1', port=1080):
        self.host = host
        self.port = port
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
    def start(self):
        self.server.bind((self.host, self.port))
        self.server.listen(100)
        print(f"🔐 SOCKS5 Proxy running on {self.host}:{self.port}")
        print(f"📡 Traffic will go through ExpressVPN")
        print(f"")
        print(f"Now run this command in another terminal to forward to UCloud:")
        print(f"  ssh -R 1080:localhost:1080 ucloud")
        print(f"")
        print(f"Press Ctrl+C to stop")
        
        while True:
            try:
                client, addr = self.server.accept()
                thread = threading.Thread(target=self.handle_client, args=(client,))
                thread.daemon = True
                thread.start()
            except KeyboardInterrupt:
                print("\n👋 Shutting down...")
                break
                
    def handle_client(self, client):
        try:
            # SOCKS5 handshake
            version, nmethods = struct.unpack('!BB', client.recv(2))
            methods = client.recv(nmethods)
            
            # No authentication
            client.send(struct.pack('!BB', 5, 0))
            
            # Get request
            version, cmd, _, atype = struct.unpack('!BBBB', client.recv(4))
            
            if atype == 1:  # IPv4
                addr = socket.inet_ntoa(client.recv(4))
            elif atype == 3:  # Domain
                domain_len = ord(client.recv(1))
                addr = client.recv(domain_len).decode()
            elif atype == 4:  # IPv6
                addr = socket.inet_ntop(socket.AF_INET6, client.recv(16))
            else:
                client.close()
                return
                
            port = struct.unpack('!H', client.recv(2))[0]
            
            # Connect to target
            try:
                remote = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                remote.settimeout(15)
                remote.connect((addr, port))
                
                # Success response
                reply = struct.pack('!BBBBIH', 5, 0, 0, 1, 0, 0)
                client.send(reply)
                
                # Forward data
                self.forward(client, remote)
            except Exception as e:
                # Connection refused
                reply = struct.pack('!BBBBIH', 5, 5, 0, 1, 0, 0)
                client.send(reply)
                
        except Exception as e:
            pass
        finally:
            client.close()
            
    def forward(self, client, remote):
        while True:
            r, w, e = select.select([client, remote], [], [], 60)
            if not r:
                break
            for sock in r:
                try:
                    data = sock.recv(4096)
                    if not data:
                        return
                    if sock is client:
                        remote.send(data)
                    else:
                        client.send(data)
                except:
                    return


if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 1080
    server = SOCKS5Server(port=port)
    server.start()

