#!/usr/bin/env python3
"""Tiny raw-ESL client: eslc.py '<api command>' [expected_substring] [-q]
Connects 127.0.0.1:8021, auths, runs an api command, prints the response.
Exit 0 when expected_substring is present (or no expectation), else 1."""
import socket
import sys
import time


def read_all(s, idle=0.5):
    s.settimeout(idle)
    buf = b""
    try:
        while True:
            d = s.recv(65536)
            if not d:
                break
            buf += d
    except socket.timeout:
        pass
    return buf


def main():
    args = [a for a in sys.argv[1:] if a != "-q"]
    quiet = "-q" in sys.argv
    cmd = args[0] if args else "status"
    expect = args[1] if len(args) > 1 else None

    s = socket.create_connection(("127.0.0.1", 8021), timeout=3)
    allbuf = read_all(s)  # banner: Content-Type: auth/request
    s.sendall(b"auth ClueCon\n\n")
    allbuf += read_all(s)  # command/reply +OK
    s.sendall(f"api {cmd}\n\n".encode())
    time.sleep(0.3)
    allbuf += read_all(s)  # api/response + body
    s.close()

    out = allbuf.decode(errors="replace")
    ok = (expect is None) or (expect in out)
    if not quiet:
        print(out)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
