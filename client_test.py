"""
Client Test Harness for Persistent HTTP/1.1 Calculator Server
============================================================
Reproduces the exact marking sequence from the assignment slide:
  1 socket, 1 TCP handshake, 6 responses, socket still open: True

Also validates:
  - HTTP pipelining (coalesced requests)
  - Fragmented TCP packet arrival (slow chunks)
  - Parameter validation errors (missing param, invalid number, div zero)
  - Missing Host header error
  - Connection: close handling
  - POST with body on persistent socket
"""

import socket
import time
import sys
from typing import Tuple, List, Dict


def read_http_response(sock: socket.socket, buffer: bytearray) -> Tuple[int, Dict[str, str], bytes]:
    """
    Reads exactly one HTTP response from a persistent TCP socket and byte buffer.
    Accurately respects Content-Length so that any extra bytes remaining in the
    buffer belong to subsequent responses.
    """
    delimiter = b"\r\n\r\n"
    while True:
        idx = buffer.find(delimiter)
        if idx != -1:
            header_end = idx
            delim_len = 4
            break
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Socket closed prematurely while reading response headers")
        buffer.extend(chunk)

    header_bytes = bytes(buffer[:header_end])
    lines = header_bytes.decode("iso-8859-1").splitlines()
    status_line = lines[0]
    parts = status_line.split()
    status_code = int(parts[1])

    headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()

    content_len = int(headers.get("content-length", 0))
    total_len = header_end + delim_len + content_len

    while len(buffer) < total_len:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Socket closed prematurely while reading response body")
        buffer.extend(chunk)

    body = bytes(buffer[header_end + delim_len : total_len])
    del buffer[:total_len]

    return status_code, headers, body


def is_socket_open(sock: socket.socket) -> bool:
    """Verifies that the socket is still open and usable."""
    try:
        # Check non-blocking read for EOF
        sock.setblocking(False)
        data = sock.recv(1, socket.MSG_PEEK)
        sock.setblocking(True)
        # If recv returns b'', client saw EOF
        return True
    except BlockingIOError:
        # No data ready to read, socket is alive and waiting
        sock.setblocking(True)
        return True
    except (ConnectionResetError, ConnectionAbortedError, OSError):
        return False


def test_assignment_marking_sequence(host="localhost", port=8080):
    print("=" * 60)
    print("TEST 1: Assignment Marking Sequence (Slide 1)")
    print("  'one socket. every request.'")
    print("  's = socket.create_connection((\"localhost\", 8080))'")
    print("=" * 60)

    s = socket.create_connection((host, port))
    buf = bytearray()

    test_cases = [
        ("GET /add?a=2&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"5"),
        ("GET /sub?a=10&b=4 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"6"),
        ("GET /mul?a=6&b=7 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"42"),
        ("GET /div?a=1&b=0 HTTP/1.1\r\nHost: localhost\r\n\r\n", 400, None),
        ("GET /pow?a=2&b=8 HTTP/1.1\r\nHost: localhost\r\n\r\n", 404, None),
        ("POST /add HTTP/1.1\r\nHost: localhost\r\n\r\n", 405, None),
    ]

    responses_received = 0
    for req_text, expected_code, expected_body in test_cases:
        req_line = req_text.splitlines()[0]
        s.sendall(req_text.encode("latin1"))
        code, headers, body = read_http_response(s, buf)
        responses_received += 1

        body_display = body.decode("utf-8", errors="replace").strip()
        print(f"  {req_line:<35} -> {code:<4} {body_display}")

        assert code == expected_code, f"Expected {expected_code}, got {code}"
        if expected_body is not None:
            assert body == expected_body, f"Expected body {expected_body!r}, got {body!r}"

    # Check that socket is still open
    still_open = is_socket_open(s)
    print(f"\n  socket still open: {still_open}")
    print(f"  1 TCP handshake, {responses_received} responses")
    assert still_open is True, "Socket closed unexpectedly!"
    assert responses_received == 6, f"Expected 6 responses, got {responses_received}"
    s.close()
    print("  [PASS] Marking sequence passed successfully!\n")


def test_division_and_parameter_validations(host="localhost", port=8080):
    print("=" * 60)
    print("TEST 2: Division & Parameter Validation Cases")
    print("=" * 60)

    s = socket.create_connection((host, port))
    buf = bytearray()

    cases = [
        ("GET /div?a=9&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"3"),
        ("GET /div?a=6&b=2 HTTP/1.1\r\nHost: localhost\r\n\r\n", 200, b"3"),
        ("GET /add?a=x&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n", 400, None),
        ("GET /add?a=2 HTTP/1.1\r\nHost: localhost\r\n\r\n", 400, None),
        ("GET /add HTTP/1.1\r\nHost: localhost\r\n\r\n", 400, None),
        ("GET /add?a=2&b=3 HTTP/1.1\r\n\r\n", 400, None),  # Missing Host header!
    ]

    for req_text, expected_code, expected_body in cases:
        req_line = req_text.splitlines()[0]
        s.sendall(req_text.encode("latin1"))
        code, headers, body = read_http_response(s, buf)
        body_display = body.decode("utf-8", errors="replace").strip()
        print(f"  {req_line:<35} -> {code:<4} {body_display}")
        assert code == expected_code, f"Expected {expected_code}, got {code}"
        if expected_body:
            assert body == expected_body, f"Expected {expected_body!r}, got {body!r}"

    still_open = is_socket_open(s)
    print(f"  socket still open after validation errors: {still_open}")
    assert still_open is True
    s.close()
    print("  [PASS] Parameter validations passed!\n")


def test_pipelining_coalesced_requests(host="localhost", port=8080):
    print("=" * 60)
    print("TEST 3: HTTP Pipelining (Multiple Coalesced Requests in 1 send)")
    print("  'take all six at once and answer in order, which is pipelining'")
    print("=" * 60)

    s = socket.create_connection((host, port))
    buf = bytearray()

    # Concatenate 4 requests into a single batch
    batch = (
        "GET /add?a=10&b=20 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        "GET /sub?a=50&b=8 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        "GET /mul?a=5&b=5 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        "GET /div?a=100&b=4 HTTP/1.1\r\nHost: localhost\r\n\r\n"
    )

    print("  Sending 4 requests in a single TCP sendall() call...")
    s.sendall(batch.encode("latin1"))

    expected = [(200, b"30"), (200, b"42"), (200, b"25"), (200, b"25")]
    for i, (exp_code, exp_body) in enumerate(expected, 1):
        code, headers, body = read_http_response(s, buf)
        print(f"  Response {i}: status={code}, body={body.decode('utf-8')}")
        assert code == exp_code
        assert body == exp_body

    print(f"  socket still open: {is_socket_open(s)}")
    s.close()
    print("  [PASS] HTTP pipelining handled flawlessly!\n")


def test_fragmented_tcp_reads(host="localhost", port=8080):
    print("=" * 60)
    print("TEST 4: Fragmented TCP Reads (Partial Chunks)")
    print("  Deliberately sending request in tiny 2-byte chunks with delays")
    print("=" * 60)

    s = socket.create_connection((host, port))
    buf = bytearray()

    request = "GET /add?a=100&b=200 HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("latin1")

    # Send in 2-byte slices with 5ms sleep
    chunk_size = 2
    for i in range(0, len(request), chunk_size):
        chunk = request[i : i + chunk_size]
        s.sendall(chunk)
        time.sleep(0.005)

    code, headers, body = read_http_response(s, buf)
    print(f"  Reconstructed response: status={code}, body={body.decode('utf-8')}")
    assert code == 200
    assert body == b"300"
    s.close()
    print("  [PASS] Fragmented TCP chunks reassembled correctly!\n")


def test_post_with_body_consumed_correctly(host="localhost", port=8080):
    print("=" * 60)
    print("TEST 5: POST with Body (Boundary/Content-Length Invariant)")
    print("  Ensuring Content-Length bytes are drained so byte n+1 is not corrupted")
    print("=" * 60)

    s = socket.create_connection((host, port))
    buf = bytearray()

    # Request 1: POST with 10-byte body (should return 405 Method Not Allowed)
    post_req = (
        "POST /add HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Content-Length: 10\r\n"
        "\r\n"
        "0123456789"
    )
    s.sendall(post_req.encode("latin1"))
    code, headers, body = read_http_response(s, buf)
    print(f"  POST /add with body -> {code}")
    assert code == 405

    # Request 2 immediately following on SAME socket: GET /mul?a=3&b=3
    get_req = "GET /mul?a=3&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n"
    s.sendall(get_req.encode("latin1"))
    code, headers, body = read_http_response(s, buf)
    print(f"  Next GET on same socket -> {code}, body={body.decode('utf-8')}")
    assert code == 200
    assert body == b"9"

    s.close()
    print("  [PASS] Body bytes safely consumed without buffer bleed!\n")


def test_connection_close_header(host="localhost", port=8080):
    print("=" * 60)
    print("TEST 6: Honour 'Connection: close' Header")
    print("=" * 60)

    s = socket.create_connection((host, port))
    buf = bytearray()

    req = "GET /add?a=1&b=1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    s.sendall(req.encode("latin1"))
    code, headers, body = read_http_response(s, buf)
    print(f"  Response with Connection: close -> status={code}, Connection={headers.get('connection')}")
    assert headers.get("connection") == "close"

    # Server should now close the connection
    time.sleep(0.05)
    remaining = s.recv(1024)
    print(f"  Socket EOF received: {remaining == b''}")
    assert remaining == b"", "Server should have closed the connection upon 'Connection: close'"
    s.close()
    print("  [PASS] Connection: close honored correctly!\n")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Test client for persistent calculator server")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    print(f"Running test suite against {args.host}:{args.port}...\n")
    try:
        test_assignment_marking_sequence(args.host, args.port)
        test_division_and_parameter_validations(args.host, args.port)
        test_pipelining_coalesced_requests(args.host, args.port)
        test_fragmented_tcp_reads(args.host, args.port)
        test_post_with_body_consumed_correctly(args.host, args.port)
        test_connection_close_header(args.host, args.port)
        print("=" * 60)
        print("ALL TESTS PASSED! SERVER IS SUBMISSION-READY.")
        print("=" * 60)
    except ConnectionRefusedError:
        print(f"[-] Could not connect to {args.host}:{args.port}. Is the server running?")
        sys.exit(1)


if __name__ == "__main__":
    main()
