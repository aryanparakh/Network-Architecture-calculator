# Persistent HTTP/1.1 Calculator Server

> **Assignment:** Early HTTP/1.1 · "Build a calculator that stays on the line"  
> **Requirement:** Pure raw TCP sockets, standard library only, zero web frameworks.

---

## Overview

This project implements a persistent HTTP/1.1 calculator server using raw TCP sockets in Python. Unlike basic HTTP servers that close the socket after sending a response (`Connection: close` / HTTP/1.0 EOF delimiter), this server keeps the TCP connection alive across multiple requests (`Connection: keep-alive`), allowing a client to execute multiple operations over a single TCP handshake.

### Key Capabilities
- **Raw Socket Implementation:** Built strictly using Python's standard library `socket` and `threading` modules. No Flask, FastAPI, Django, or external libraries.
- **Persistent Connection (Keep-Alive):** A single TCP connection handles sequential or pipelined requests without terminating the socket.
- **TCP Stream Framing & Buffering:** Maintains a per-client byte buffer to correctly handle partial reads, packet fragmentation, and coalesced/pipelined requests.
- **Strict Boundary Delimitation:** Consumes exactly the `Content-Length` bytes of any request body, ensuring no bytes bleed into subsequent requests.
- **RFC 7230 / HTTP/1.1 Compliance:** Enforces the mandatory `Host` header, supports `Connection: close` and `Connection: keep-alive`, and returns standard status codes (`200`, `400`, `404`, `405`).

---

## Architecture & Framing Mechanics

### Why Sockets Stay Open (HTTP/1.0 vs. HTTP/1.1)

In HTTP/1.0, servers typically handled request boundaries by closing the TCP connection after sending the response:
```
Client: Connect -> Request -> Server: Response -> Server Closes Socket (EOF)
```
In HTTP/1.1, connections are **persistent by default**. Closing and re-opening sockets introduces significant latency (TCP 3-way handshakes, slow start). 

However, persistent connections introduce the fundamental problem of stream framing: **How does the server know where one request ends and the next begins without seeing an EOF?**

### The Byte Stream Problem & The Receive Buffer

TCP is an unstructured byte stream. A single call to `sock.recv(4096)` can return:
1. **A partial request** (e.g., headers split across multiple network packets).
2. **Exactly one request**.
3. **Multiple requests coalesced together** (pipelining or network batching).
4. **One complete request plus the prefix of the next request**.

```
TCP Byte Stream:
[ Request 1 Headers \r\n\r\n ][ Request 1 Body ][ Request 2 Headers \r\n\r\n ]...
|<------------------ Request 1 --------------->|<--------- Request 2 ------->
```

### Framing Algorithm in `server.py`

Each client connection is assigned a persistent `bytearray` buffer managed by `PersistentClientWorker`:

1. **Header Delimiter Search:** The server scans the buffer for `b"\r\n\r\n"` (or `b"\n\n"` fallback). If not found, it calls `sock.recv()` until the complete header section arrives.
2. **Content-Length Inspection:** Once headers are located, the server parses `Content-Length`. If a body is present (e.g., in a `POST`), the total request length is calculated as:
   $$\text{Total Length} = \text{Header End} + 4 + \text{Content-Length}$$
3. **Body Accumulation:** The server continues receiving bytes until the buffer holds at least `Total Length` bytes.
4. **Exact Slicing:** The server extracts exactly `buffer[:Total Length]` for parsing and removes those bytes from the buffer using `del buffer[:Total Length]`. **Byte $N+1$ remains in the buffer and belongs to the next request.**
5. **Immediate Pipelining Check:** Before blocking on `recv()`, the loop checks if the remaining buffer already contains another complete request. If so, it processes it immediately without waiting for network I/O.

---

## API Reference

### Calculator Endpoints (`GET`)

All arithmetic endpoints accept query parameters `a` and `b`.

| Method | Endpoint | Query Parameters | Success Status | Example Response | Description |
|---|---|---|---|---|---|
| `GET` | `/add` | `?a=2&b=3` | `200 OK` | `5` | Returns $a + b$ |
| `GET` | `/sub` | `?a=10&b=4` | `200 OK` | `6` | Returns $a - b$ |
| `GET` | `/mul` | `?a=6&b=7` | `200 OK` | `42` | Returns $a \times b$ |
| `GET` | `/div` | `?a=9&b=3` | `200 OK` | `3` | Returns $a / b$ (integer formatted if whole) |

### Error Cases & Status Codes

| Request | Expected Status | Reason | Connection State |
|---|---|---|---|
| `GET /div?a=1&b=0` | `400 Bad Request` | Division by zero | **Kept Open** |
| `GET /add?a=x&b=3` | `400 Bad Request` | Non-numeric parameter | **Kept Open** |
| `GET /add?a=2` | `400 Bad Request` | Missing required parameter `b` | **Kept Open** |
| `GET /add (no Host)` | `400 Bad Request` | Missing mandatory `Host` header | **Kept Open** |
| `GET /pow?a=2&b=8` | `404 Not Found` | Unsupported operation | **Kept Open** |
| `POST /add` | `405 Method Not Allowed` | Unsupported method (only `GET` allowed) | **Kept Open** |
| Corrupted / Unparseable | `400 Bad Request` | Stream desynchronization | Closed |
| `Connection: close` | Status `200`/`4xx` | Explicit client closure request | Closed cleanly |

> **Important:** Application-level errors (`400`, `404`, `405`) **do not close the socket**. The client remains connected and can immediately execute further requests.

---

## Project Structure

```
Network Architecture Assignment/
├── server.py              # Persistent HTTP/1.1 server using raw TCP sockets
├── client_test.py         # Client test harness replicating exact marking script
├── tests/
│   ├── __init__.py
│   └── test_server.py     # Automated unittest test suite (18 unit & integration tests)
└── README.md              # Documentation and specifications
```

---

## How to Run

### Starting the Server

```bash
python server.py
```

Optional CLI flags:
- `--host`: Host to bind (default: `0.0.0.0`)
- `--port`: Port to listen on (default: `8080`)
- `--timeout`: Client idle timeout in seconds (default: `30.0`)

Example:
```bash
python server.py --host 127.0.0.1 --port 8080 --timeout 60
```

---

## How to Test

### 1. Run the Slide Marking Harness (`client_test.py`)

In a second terminal window (with `server.py` running):

```bash
python client_test.py
```

This runs the exact marking test from Slide 1:
```text
s = socket.create_connection(("localhost", 8080))

GET /add?a=2&b=3   -> 200 5
GET /sub?a=10&b=4  -> 200 6
GET /mul?a=6&b=7   -> 200 42
GET /div?a=1&b=0   -> 400 Bad Request: Division by zero
GET /pow?a=2&b=8   -> 404 Not Found
POST /add          -> 405 Method Not Allowed

socket still open: True
1 TCP handshake, 6 responses
```

It also automatically executes tests for:
- Parameter validation and missing `Host` headers
- HTTP pipelining (multiple requests batched into a single `sendall`)
- Fragmented TCP packet arrival (deliberate 2-byte chunk streaming)
- `POST` requests with a body (verifying the body is drained without leaking into the next request)
- `Connection: close` header handling

### 2. Run the Full Automated Test Suite (`unittest`)

Runs standalone unit and integration tests without needing to manually launch the server (the test runner manages a background test server instance on an ephemeral port):

```bash
python -m unittest discover tests
```
Or:
```bash
python tests/test_server.py
```

Output:
```text
..................
----------------------------------------------------------------------
Ran 18 tests in 0.204s

OK
```

### 3. Testing with `curl` (Persistent Connection Verification)

You can verify connection persistence using `curl` with verbose output (`-v`). Passing multiple URLs instructs `curl` to reuse the existing TCP connection:

```bash
curl -i -v "http://localhost:8080/add?a=2&b=3" "http://localhost:8080/sub?a=10&b=4" "http://localhost:8080/mul?a=6&b=7"
```

Notice in `curl`'s stderr:
```text
* Established connection to localhost (127.0.0.1 port 8080)
* Connection #0 to host localhost:8080 left intact
* Reusing existing http: connection with host localhost
* Connection #0 to host localhost:8080 left intact
* Reusing existing http: connection with host localhost
* Connection #0 to host localhost:8080 left intact
```
Only **one connection** is established for all three requests!

---

## Example: Raw Socket Python Client

Here is how you can interact with the server in pure Python using a single socket:

```python
import socket

# 1. Single TCP handshake
s = socket.create_connection(("localhost", 8080))

def send_and_receive(request_bytes):
    s.sendall(request_bytes)
    # Read until header terminator
    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        buf.extend(s.recv(1024))
    
    header_end = buf.find(b"\r\n\r\n")
    headers = buf[:header_end].decode("latin1")
    
    # Extract Content-Length
    content_len = 0
    for line in headers.splitlines():
        if line.lower().startswith("content-length:"):
            content_len = int(line.split(":")[1].strip())
            
    # Read remaining body bytes
    total = header_end + 4 + content_len
    while len(buf) < total:
        buf.extend(s.recv(1024))
        
    body = buf[header_end + 4 : total].decode("utf-8")
    return headers.splitlines()[0], body

# Send multiple requests over the SAME socket
status, body = send_and_receive(b"GET /add?a=2&b=3 HTTP/1.1\r\nHost: localhost\r\n\r\n")
print(f"Request 1: {status} -> {body}")

status, body = send_and_receive(b"GET /mul?a=6&b=7 HTTP/1.1\r\nHost: localhost\r\n\r\n")
print(f"Request 2: {status} -> {body}")

# Clean closure
s.close()
```

---

## Note Regarding Page 2 of the PDF

The attached assignment PDF also displays a preview slide for a course project ("HTTP, IN BINARY · TWO TRACKS, ONE PROTOCOL: `./bserve` and `./bcurl`"). Per assignment instructions, that is a separate binary protocol project and is intentionally kept distinct from this HTTP/1.1 Persistent Calculator assignment.
