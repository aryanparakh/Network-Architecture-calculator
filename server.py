"""
HTTP/1.1 Persistent Connection Calculator Server
================================================
Implemented using raw TCP sockets (no web frameworks, standard library only).
Supports persistent connections (HTTP/1.1 keep-alive), request stream framing,
pipelining, parameter validation, and calculator endpoints.
"""

import sys
import socket
import threading
import urllib.parse
from dataclasses import dataclass
from typing import Dict, Tuple, Optional


# HTTP Status Phrases
STATUS_PHRASES: Dict[int, str] = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    500: "Internal Server Error",
}


@dataclass
class HTTPRequest:
    """Represents a fully parsed HTTP request."""
    method: str
    target: str
    path: str
    query_params: Dict[str, list]
    version: str
    headers: Dict[str, str]
    body: bytes


@dataclass
class HTTPResponse:
    """Represents an HTTP response to be serialized and sent over TCP."""
    status_code: int
    headers: Dict[str, str]
    body: bytes

    def to_bytes(self) -> bytes:
        reason = STATUS_PHRASES.get(self.status_code, "Unknown")
        lines = [f"{'HTTP/1.1'} {self.status_code} {reason}\r\n"]
        for key, val in self.headers.items():
            lines.append(f"{key}: {val}\r\n")
        lines.append("\r\n")
        header_bytes = "".join(lines).encode("latin1")
        return header_bytes + self.body


class HTTPRequestParser:
    """
    Robust HTTP/1.1 stream parser that tracks boundaries inside a persistent
    byte buffer without consuming bytes belonging to subsequent requests.
    """

    @staticmethod
    def inspect_buffer(buf: bytearray) -> Optional[Tuple[int, int]]:
        """
        Inspects the buffer to check if a complete HTTP request has arrived.
        
        Returns:
            Tuple[int, int]: (header_end_idx, total_request_len) if complete,
            None if more bytes are needed from the socket.
        """
        # Look for standard CRLF CRLF header terminator
        header_end = buf.find(b"\r\n\r\n")
        delimiter_len = 4

        # Fallback check for LF LF
        if header_end == -1:
            header_end = buf.find(b"\n\n")
            delimiter_len = 2

        if header_end == -1:
            return None  # Headers not complete yet

        # Headers are complete, now determine expected body length
        header_bytes = bytes(buf[:header_end])
        content_length = 0

        # Scan for Content-Length in header lines (case-insensitive)
        for line in header_bytes.split(b"\n"):
            line = line.strip()
            if line.lower().startswith(b"content-length:"):
                parts = line.split(b":", 1)
                if len(parts) == 2:
                    try:
                        content_length = int(parts[1].strip())
                        if content_length < 0:
                            content_length = 0
                    except ValueError:
                        content_length = 0

        total_req_len = header_end + delimiter_len + content_length

        if len(buf) < total_req_len:
            return None  # Body incomplete, need more data

        return (header_end, total_req_len)

    @staticmethod
    def parse(raw_request: bytes, header_end: int) -> HTTPRequest:
        """
        Parses raw bytes of a single framed request into an HTTPRequest object.
        """
        # Locate delimiter
        if b"\r\n\r\n" in raw_request[:header_end + 4]:
            delimiter_len = 4
        else:
            delimiter_len = 2

        header_bytes = raw_request[:header_end]
        body = raw_request[header_end + delimiter_len:]

        header_text = header_bytes.decode("iso-8859-1")
        lines = header_text.splitlines()
        if not lines:
            raise ValueError("Empty request")

        request_line = lines[0].strip()
        parts = request_line.split()
        if len(parts) < 2:
            raise ValueError(f"Malformed request line: {request_line}")

        method = parts[0].upper()
        target = parts[1]
        version = parts[2].upper() if len(parts) >= 3 else "HTTP/1.0"

        # Parse headers into case-insensitive dict (keys lowercase)
        headers: Dict[str, str] = {}
        for line in lines[1:]:
            if not line.strip():
                continue
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        # Parse URL target and query parameters
        parsed_url = urllib.parse.urlsplit(target)
        path = parsed_url.path
        query_params = urllib.parse.parse_qs(parsed_url.query, keep_blank_values=True)

        return HTTPRequest(
            method=method,
            target=target,
            path=path,
            query_params=query_params,
            version=version,
            headers=headers,
            body=body,
        )


class CalculatorHandler:
    """
    Handles calculator business logic and validates HTTP/1.1 requirements.
    """

    ALLOWED_OPERATIONS = {"/add", "/sub", "/mul", "/div"}

    @classmethod
    def handle_request(cls, req: HTTPRequest) -> Tuple[HTTPResponse, bool]:
        """
        Evaluates the HTTPRequest and returns (HTTPResponse, should_close).
        """
        # Determine connection preference
        conn_header = req.headers.get("connection", "").lower()
        if req.version == "HTTP/1.1":
            # HTTP/1.1 defaults to keep-alive unless 'close' is specified
            should_close = (conn_header == "close")
        else:
            # HTTP/1.0 defaults to close unless 'keep-alive' is specified
            should_close = (conn_header != "keep-alive")

        # 1. Check HTTP/1.1 Host header requirement
        # RFC 7230 / RFC 9112: HTTP/1.1 requests MUST include a Host header
        if req.version == "HTTP/1.1" and "host" not in req.headers:
            return cls._error_response(400, "Bad Request: Missing Host header", should_close)

        # 2. Check HTTP Method
        if req.method != "GET":
            # Assignment requirement: POST /add -> 405 Method Not Allowed
            return cls._method_not_allowed_response(should_close)

        # 3. Check Endpoint
        if req.path not in cls.ALLOWED_OPERATIONS:
            # Assignment requirement: GET /pow?a=2&b=8 -> 404 Not Found
            return cls._error_response(404, "Not Found", should_close)

        # 4. Check Query Parameters ('a' and 'b')
        if "a" not in req.query_params or "b" not in req.query_params:
            return cls._error_response(400, "Bad Request: Missing required parameter 'a' or 'b'", should_close)

        val_a_str = req.query_params["a"][0]
        val_b_str = req.query_params["b"][0]

        try:
            num_a = cls._parse_number(val_a_str)
            num_b = cls._parse_number(val_b_str)
        except ValueError:
            # Invalid numeric parameter: e.g. a=x
            return cls._error_response(400, "Bad Request: Invalid numeric parameter", should_close)

        # 5. Perform Arithmetic
        if req.path == "/add":
            result = num_a + num_b
        elif req.path == "/sub":
            result = num_a - num_b
        elif req.path == "/mul":
            result = num_a * num_b
        elif req.path == "/div":
            if num_b == 0:
                # Division by zero: e.g. /div?a=1&b=0 -> 400
                return cls._error_response(400, "Bad Request: Division by zero", should_close)
            if isinstance(num_a, int) and isinstance(num_b, int):
                if num_a % num_b == 0:
                    result = num_a // num_b
                else:
                    result = num_a / num_b
            else:
                result = num_a / num_b
        else:
            return cls._error_response(404, "Not Found", should_close)

        # Format number: integer without decimal if whole number
        if isinstance(result, float) and result.is_integer():
            result_str = str(int(result))
        else:
            result_str = str(result)

        body_bytes = result_str.encode("utf-8")
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(body_bytes)),
            "Connection": "close" if should_close else "keep-alive",
        }

        return HTTPResponse(status_code=200, headers=headers, body=body_bytes), should_close

    @staticmethod
    def _parse_number(s: str):
        """Parses an integer or floating point number from string."""
        s = s.strip()
        try:
            return int(s)
        except ValueError:
            return float(s)

    @classmethod
    def _error_response(cls, status_code: int, message: str, should_close: bool) -> Tuple[HTTPResponse, bool]:
        body_bytes = f"{message}\n".encode("utf-8")
        headers = {
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(body_bytes)),
            "Connection": "close" if should_close else "keep-alive",
        }
        return HTTPResponse(status_code=status_code, headers=headers, body=body_bytes), should_close

    @classmethod
    def _method_not_allowed_response(cls, should_close: bool) -> Tuple[HTTPResponse, bool]:
        body_bytes = b"Method Not Allowed\n"
        headers = {
            "Allow": "GET",
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(body_bytes)),
            "Connection": "close" if should_close else "keep-alive",
        }
        return HTTPResponse(status_code=405, headers=headers, body=body_bytes), should_close


class PersistentClientWorker:
    """
    Manages a single client's raw TCP socket lifecycle.
    Maintains a per-client receive buffer to guarantee that:
      1. Partial TCP reads are accumulated until a complete request exists.
      2. Multiple requests in one read (pipelining/coalescing) are handled sequentially.
      3. Exactly Content-Length bytes are consumed without leaking into byte n+1.
      4. The socket remains open for subsequent requests.
    """

    RECV_CHUNK_SIZE = 4096

    def __init__(self, client_sock: socket.socket, client_addr: Tuple[str, int], idle_timeout: float = 30.0):
        self.sock = client_sock
        self.addr = client_addr
        self.idle_timeout = idle_timeout
        self.buffer = bytearray()

    def run(self):
        """Performs the persistent request-response loop."""
        try:
            self.sock.settimeout(self.idle_timeout)

            while True:
                # STEP 1: Check if buffer already contains a complete request.
                # If not, read from socket until we have a complete request or EOF.
                framing = HTTPRequestParser.inspect_buffer(self.buffer)

                while framing is None:
                    try:
                        chunk = self.sock.recv(self.RECV_CHUNK_SIZE)
                    except socket.timeout:
                        # Idle timeout expired: close connection cleanly
                        return
                    except (ConnectionResetError, ConnectionAbortedError, OSError):
                        return

                    if not chunk:
                        # Client performed orderly shutdown (EOF)
                        return

                    self.buffer.extend(chunk)
                    framing = HTTPRequestParser.inspect_buffer(self.buffer)

                header_end, total_req_len = framing

                # STEP 2: Extract EXACTLY this request's bytes.
                # Crucial invariant: bytes beyond total_req_len belong to the next request!
                raw_request = bytes(self.buffer[:total_req_len])
                del self.buffer[:total_req_len]

                # STEP 3: Parse the request
                try:
                    request = HTTPRequestParser.parse(raw_request, header_end)
                    response, should_close = CalculatorHandler.handle_request(request)
                except Exception:
                    # Malformed HTTP request
                    response, should_close = CalculatorHandler._error_response(
                        400, "Bad Request: Malformed HTTP request", should_close=True
                    )

                # STEP 4: Send the response back through the SAME socket
                try:
                    self.sock.sendall(response.to_bytes())
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError):
                    return

                # STEP 5: If client requested closure or fatal error occurred, finish loop
                if should_close:
                    return

        finally:
            try:
                self.sock.close()
            except OSError:
                pass


class CalculatorServer:
    """
    Raw TCP Socket Server that binds, listens, and spawns persistent client workers.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8080, idle_timeout: float = 30.0):
        self.host = host
        self.port = port
        self.idle_timeout = idle_timeout
        self.server_sock: Optional[socket.socket] = None
        self.is_running = False

    def start(self):
        """Binds socket and listens for incoming connections."""
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(128)
        self.is_running = True

        print(f"[*] Persistent Calculator Server listening on {self.host}:{self.port}")
        print(f"[*] Idle timeout set to {self.idle_timeout}s")
        print("[*] Ready to accept persistent connections.\n")

        try:
            while self.is_running:
                try:
                    client_sock, client_addr = self.server_sock.accept()
                except OSError:
                    if not self.is_running:
                        break
                    raise

                worker = PersistentClientWorker(client_sock, client_addr, self.idle_timeout)
                client_thread = threading.Thread(target=worker.run, daemon=True)
                client_thread.start()
        except KeyboardInterrupt:
            print("\n[*] Server stopping...")
        finally:
            self.stop()

    def stop(self):
        """Stops the server socket."""
        self.is_running = False
        if self.server_sock:
            try:
                self.server_sock.close()
            except OSError:
                pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Persistent HTTP/1.1 Calculator Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host interface to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")
    parser.add_argument("--timeout", type=float, default=30.0, help="Client idle timeout in seconds (default: 30.0)")
    args = parser.parse_args()

    server = CalculatorServer(host=args.host, port=args.port, idle_timeout=args.timeout)
    server.start()


if __name__ == "__main__":
    main()
